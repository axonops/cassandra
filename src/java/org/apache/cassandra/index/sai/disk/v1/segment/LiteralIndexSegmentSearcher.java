/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.disk.v1.segment;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.analyzer.AnalyzedToken;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.disk.v1.postings.PhrasePostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.PositionsReader;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.metrics.MulticastQueryEventListeners;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.store.IndexInput;

/**
 * Executes {@link Expression}s against the trie-based terms dictionary for an individual index segment.
 */
public class LiteralIndexSegmentSearcher extends IndexSegmentSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(LiteralIndexSegmentSearcher.class);

    private final LiteralIndexSegmentTermsReader reader;
    private final QueryEventListener.TrieIndexEventListener perColumnEventListener;
    @Nullable
    private final FileHandle phrasePostingsFile;
    @Nullable
    private final FileHandle phrasePositionsFile;

    LiteralIndexSegmentSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                                PerColumnIndexFiles perIndexFiles,
                                SegmentMetadata segmentMetadata,
                                StorageAttachedIndex index) throws IOException
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, index);

        long root = metadata.getIndexRoot(IndexComponent.TERMS_DATA);
        assert root >= 0;

        perColumnEventListener = (QueryEventListener.TrieIndexEventListener)index.columnQueryMetrics();

        Map<String,String> map = metadata.componentMetadatas.get(IndexComponent.TERMS_DATA).attributes;
        String footerPointerString = map.get(SAICodecUtils.FOOTER_POINTER);
        long footerPointer = footerPointerString == null ? -1 : Long.parseLong(footerPointerString);

        reader = new LiteralIndexSegmentTermsReader(index.identifier(), indexFiles.termsData(), indexFiles.postingLists(), root, footerPointer);
        // Only analyzed indexes have a positions component and serve phrase queries
        phrasePostingsFile = index.hasLuceneAnalyzer() ? indexFiles.postingLists() : null;
        phrasePositionsFile = index.hasLuceneAnalyzer() ? indexFiles.positions() : null;
    }

    @Override
    public long indexFileCacheSize()
    {
        // trie has no pre-allocated memory.
        return 0;
    }

    @Override
    public KeyRangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange, QueryContext queryContext) throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace(index.identifier().logMessage("Searching on expression '{}'..."), expression);

        if (expression.matchesNothing())
            return KeyRangeIterator.empty();

        if (expression.getIndexOperator() == Expression.IndexOperator.PHRASE)
            return toPrimaryKeyIterator(phraseMatch(expression, queryContext), queryContext);

        if (!expression.getIndexOperator().isEquality() && expression.getIndexOperator() != Expression.IndexOperator.ANALYZER_MATCHES)
            throw new IllegalArgumentException(index.identifier().logMessage("Unsupported expression: " + expression));

        ByteComparable term = v -> index.termType().asComparableBytes(expression.lower().value.encoded, v);
        QueryEventListener.TrieIndexEventListener listener = MulticastQueryEventListeners.of(queryContext, perColumnEventListener);
        return toPrimaryKeyIterator(reader.exactMatch(term, listener, queryContext), queryContext);
    }

    /**
     * Builds the lazy phrase intersection over the segment: one posting list and one positions
     * reader per distinct phrase token, leapfrogged by {@link PhrasePostingList}. A token the
     * segment does not contain means the phrase matches nothing here.
     */
    @Nullable
    private PostingList phraseMatch(Expression expression, QueryContext queryContext) throws IOException
    {
        List<AnalyzedToken> queryTokens = expression.phraseTokens();

        Map<ByteBuffer, Integer> distinctTokens = new LinkedHashMap<>();
        for (AnalyzedToken token : queryTokens)
            distinctTokens.putIfAbsent(token.bytes(), distinctTokens.size());

        int[] occurrenceToken = new int[queryTokens.size()];
        int[] queryPositions = new int[queryTokens.size()];
        for (int occurrence = 0; occurrence < queryTokens.size(); occurrence++)
        {
            AnalyzedToken token = queryTokens.get(occurrence);
            occurrenceToken[occurrence] = distinctTokens.get(token.bytes());
            queryPositions[occurrence] = token.position();
        }

        QueryEventListener.TrieIndexEventListener listener = MulticastQueryEventListeners.of(queryContext, perColumnEventListener);
        listener.onSegmentHit();

        PostingsReader[] postingsPerToken = new PostingsReader[distinctTokens.size()];
        PositionsReader[] positionsPerToken = new PositionsReader[distinctTokens.size()];
        List<Closeable> openedInputs = new ArrayList<>();
        try
        {
            int tokenIndex = 0;
            for (ByteBuffer tokenBytes : distinctTokens.keySet())
            {
                ByteComparable term = v -> index.termType().asComparableBytes(tokenBytes, v);
                long postingsOffset = reader.postingsOffset(term);
                if (postingsOffset == PostingList.OFFSET_NOT_FOUND)
                {
                    FileUtils.closeQuietly(openedInputs);
                    return null;
                }

                IndexInput summaryInput = IndexFileUtils.instance.openInput(phrasePostingsFile);
                openedInputs.add(summaryInput);
                // The trailing VLong of the format version ab postings summary points at the
                // term's positions summary. It is read before the summary parse below takes over
                // the same input for its random access reads.
                long positionsSummaryOffset = PositionsReader.readPositionsSummaryOffset(summaryInput, postingsOffset);

                IndexInput blocksInput = IndexFileUtils.instance.openInput(phrasePostingsFile);
                openedInputs.add(blocksInput);
                PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(summaryInput, postingsOffset);
                postingsPerToken[tokenIndex] = new PostingsReader(blocksInput, summary, listener.postingListEventListener());

                IndexInput positionsInput = IndexFileUtils.instance.openInput(phrasePositionsFile);
                openedInputs.add(positionsInput);
                positionsPerToken[tokenIndex] = new PositionsReader(positionsInput, positionsSummaryOffset);
                tokenIndex++;
            }
            return new PhrasePostingList(index.identifier(), postingsPerToken, positionsPerToken, occurrenceToken, queryPositions);
        }
        catch (Throwable e)
        {
            FileUtils.closeQuietly(openedInputs);
            throw e;
        }
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this).add("index", index).toString();
    }

    @Override
    public void close()
    {
        reader.close();
        FileUtils.closeQuietly(phrasePostingsFile);
        FileUtils.closeQuietly(phrasePositionsFile);
    }
}
