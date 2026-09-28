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
package org.apache.cassandra.index.sai.disk.v1.postings;

import java.io.IOException;
import java.util.Arrays;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.analyzer.PhraseMatcher;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;

/**
 * Lazily intersects one phrase's per-token posting lists inside a single segment. Postings are
 * leapfrogged to common segment row ids and each common row id survives only when the tokens'
 * stored positions satisfy the strict, gap-preserving adjacency of {@link PhraseMatcher}. Rows are
 * therefore only materialized after positions agree, and the intersection never rides the top
 * level intersection iterator with its clause limit.
 */
@NotThreadSafe
public class PhrasePostingList implements PostingList
{
    private final IndexIdentifier indexIdentifier;
    // One entry per distinct phrase token
    private final PostingsReader[] postingsPerToken;
    private final PositionsReader[] positionsPerToken;
    private final long[] currentRow;
    private final int[] currentOrdinal;
    private final int[][] tokenPositions;
    // One entry per query token occurrence, pointing into the distinct token arrays
    private final int[] occurrenceToken;
    private final int[] queryPositions;
    private final int[][] positionsPerOccurrence;

    private long lastReturned = -1;
    private long candidates;
    private long matches;

    /**
     * @param indexIdentifier the index, named in the tracing event this list emits on close
     * @param postingsPerToken one posting list per distinct phrase token, owned by this list
     * @param positionsPerToken the matching positions readers, owned by this list
     * @param occurrenceToken for each query token occurrence, its index into the distinct arrays
     * @param queryPositions for each query token occurrence, its query analyzer position
     */
    public PhrasePostingList(IndexIdentifier indexIdentifier,
                             PostingsReader[] postingsPerToken,
                             PositionsReader[] positionsPerToken,
                             int[] occurrenceToken,
                             int[] queryPositions)
    {
        assert postingsPerToken.length > 0 && occurrenceToken.length == queryPositions.length;

        this.indexIdentifier = indexIdentifier;
        this.postingsPerToken = postingsPerToken;
        this.positionsPerToken = positionsPerToken;
        this.occurrenceToken = occurrenceToken;
        this.queryPositions = queryPositions;
        currentRow = new long[postingsPerToken.length];
        Arrays.fill(currentRow, -1);
        currentOrdinal = new int[postingsPerToken.length];
        tokenPositions = new int[postingsPerToken.length][];
        positionsPerOccurrence = new int[occurrenceToken.length][];
    }

    @Override
    public long size()
    {
        // An upper bound, like every merged posting list: the phrase can only match rows that
        // contain its rarest token
        long size = Long.MAX_VALUE;
        for (PostingsReader postings : postingsPerToken)
            size = Math.min(size, postings.size());
        return size;
    }

    @Override
    public long nextPosting() throws IOException
    {
        return findNextMatch(lastReturned + 1);
    }

    @Override
    public long advance(long targetRowID) throws IOException
    {
        return findNextMatch(Math.max(targetRowID, lastReturned + 1));
    }

    @Override
    public void close()
    {
        if (Tracing.isTracing())
            Tracing.trace("Phrase intersection on index {} segment matched {} of {} candidates for {} tokens",
                          indexIdentifier.indexName, matches, candidates, occurrenceToken.length);

        FileUtils.closeQuietly(Arrays.asList(postingsPerToken));
        FileUtils.closeQuietly(Arrays.asList(positionsPerToken));
    }

    private long findNextMatch(long target) throws IOException
    {
        long candidate = nextAlignedRow(target);
        while (candidate != END_OF_STREAM)
        {
            candidates++;
            if (positionsMatch())
            {
                matches++;
                lastReturned = candidate;
                return candidate;
            }
            candidate = nextAlignedRow(candidate + 1);
        }
        lastReturned = END_OF_STREAM;
        return END_OF_STREAM;
    }

    /**
     * Leapfrogs every token's posting list to the next row id they all contain.
     */
    private long nextAlignedRow(long target) throws IOException
    {
        long candidate = advanceToken(0, target);
        outer:
        while (candidate != END_OF_STREAM)
        {
            for (int token = 1; token < postingsPerToken.length; token++)
            {
                long row = advanceToken(token, candidate);
                if (row == END_OF_STREAM)
                    return END_OF_STREAM;

                if (row != candidate)
                {
                    candidate = advanceToken(0, row);
                    continue outer;
                }
            }
            return candidate;
        }
        return END_OF_STREAM;
    }

    private long advanceToken(int token, long target) throws IOException
    {
        if (currentRow[token] >= target)
            return currentRow[token];

        long row = postingsPerToken[token].advance(target);
        currentRow[token] = row;
        // The ordinal is the count of postings consumed, so the current posting is one behind it
        currentOrdinal[token] = row == END_OF_STREAM ? -1 : Math.toIntExact(postingsPerToken[token].getOrdinal()) - 1;
        return row;
    }

    private boolean positionsMatch() throws IOException
    {
        for (int token = 0; token < positionsPerToken.length; token++)
            tokenPositions[token] = positionsPerToken[token].positions(currentOrdinal[token]);

        for (int occurrence = 0; occurrence < occurrenceToken.length; occurrence++)
            positionsPerOccurrence[occurrence] = tokenPositions[occurrenceToken[occurrence]];

        return PhraseMatcher.matches(queryPositions, positionsPerOccurrence);
    }
}
