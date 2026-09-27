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
package org.apache.cassandra.index.sai.cql;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexValidation;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.TermsScanner;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.trie.LiteralIndexWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Write, flush and compact analyzed indexes end to end and verify the on-disk result at every
 * stage. Queries through the analyzed operators are P4, so verification reads the components and
 * segment metadata directly and only single token values are asserted through {@code =}.
 */
public class AnalyzedIndexLifecycleTest extends SAITester
{
    @Before
    public void setup()
    {
        requireNetwork();
    }

    @Test
    public void writeFlushCompactKeepsAnalyzedIndexValid()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        String indexName = createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        for (int i = 0; i < 4; i++)
            execute("INSERT INTO %s (id, body) VALUES (?, ?)", i, "quick brown fox " + i);
        flush();
        verifyAnalyzedSSTables(indexName, 1);
        assertEquals(4, docCount(indexName));

        for (int i = 4; i < 8; i++)
            execute("INSERT INTO %s (id, body) VALUES (?, ?)", i, "lazy dog " + i);
        flush();
        verifyAnalyzedSSTables(indexName, 2);
        assertEquals(8, docCount(indexName));

        compact();
        waitForCompactionsFinished();
        verifyAnalyzedSSTables(indexName, 1);
        assertEquals(8, docCount(indexName));
        assertEquals(4 * 4 + 4 * 3, sumDocLengths(indexName));

        assertEquals(1, execute("SELECT * FROM %s WHERE id = 1").size());
    }

    @Test
    public void overwriteAndDeleteChainDropsStaleEntriesAtFlush()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        String indexName = createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'quick fox')");
        execute("INSERT INTO %s (id, body) VALUES (1, 'lazy dog')");
        execute("INSERT INTO %s (id, body) VALUES (2, 'brown fox')");
        execute("DELETE FROM %s WHERE id = 2");
        flush();

        // the overwritten value's terms and the deleted row are dropped together at flush
        assertEquals(Arrays.asList("dog", "lazy"), flushedTerms(indexName));
        assertEquals(1, docCount(indexName));
        assertEquals(2, sumDocLengths(indexName));

        compact();
        waitForCompactionsFinished();
        verifyAnalyzedSSTables(indexName, 1);
        assertEquals(Arrays.asList("dog", "lazy"), flushedTerms(indexName));
        assertEquals(1, docCount(indexName));
    }

    @Test
    public void collectionPartialUpdateKeepsEarlierElements()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val set<text>)");
        String indexName = createIndex("CREATE INDEX ON %s(val) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, val) VALUES (1, {'quick fox'})");
        // the partial update arrives as its own mutation, so the earlier element has an older epoch
        execute("UPDATE %s SET val = val + {'lazy dog'} WHERE id = 1");
        flush();

        // collections are excluded from the stale epoch filter, both elements' terms flush
        assertEquals(Arrays.asList("dog", "fox", "lazy", "quick"), flushedTerms(indexName));

        compact();
        waitForCompactionsFinished();

        // compaction re-analyzes the fully merged row and rebuilds exact stats
        assertEquals(Arrays.asList("dog", "fox", "lazy", "quick"), flushedTerms(indexName));
        assertEquals(1, docCount(indexName));
        assertEquals(4, sumDocLengths(indexName));
    }

    @Test
    public void expiryBeforeFlushDropsEntriesWithTheDeadRow() throws Exception
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        String indexName = createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'quick fox') USING TTL 1");
        TimeUnit.SECONDS.sleep(2);

        // a row with no live data left is excluded from the flush row mapping, so its memtable
        // entries drop out of postings, positions and doc lengths together
        flush();
        assertEquals(0, analyzedIndex(indexName).cellCount());
    }

    @Test
    public void expiryAfterFlushDropsEntriesAtCompaction() throws Exception
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        String indexName = createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'quick fox') USING TTL 4");
        flush();
        assertTrue(analyzedIndex(indexName).cellCount() > 0);

        TimeUnit.SECONDS.sleep(5);

        // compaction re-reads the value with its own nowInSec and the expired cell drops out
        compact();
        waitForCompactionsFinished();
        assertEquals(0, analyzedIndex(indexName).cellCount());
    }

    @Test
    public void plainIndexKeepsStockVersionBesideAnalyzedIndex()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v1 text, v2 text)");
        String plainName = createIndex("CREATE INDEX ON %s(v1) USING 'sai'");
        String analyzedName = createIndex("CREATE INDEX ON %s(v2) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, v1, v2) VALUES (1, 'apple', 'quick fox')");
        flush();

        StorageAttachedIndex plain = analyzedIndex(plainName);
        StorageAttachedIndex analyzed = analyzedIndex(analyzedName);

        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            IndexDescriptor indexDescriptor = IndexDescriptor.create(sstable);
            assertEquals(Version.AA, indexDescriptor.version);
            assertEquals(Version.AA, indexDescriptor.perIndexVersion(plain.identifier()));
            assertEquals(Version.AB, indexDescriptor.perIndexVersion(analyzed.identifier()));

            // the plain index stays byte-identical to stock, version aa names and no positions
            assertTrue(indexDescriptor.fileFor(IndexComponent.POSTING_LISTS, plain.identifier()).name().contains("SAI+aa+"));
            assertFalse(indexDescriptor.fileFor(IndexComponent.POSITIONS, plain.identifier()).exists());

            assertTrue(indexDescriptor.fileFor(IndexComponent.POSTING_LISTS, analyzed.identifier()).name().contains("SAI+ab+"));
            assertTrue(indexDescriptor.hasComponent(IndexComponent.POSITIONS, analyzed.identifier()));
            assertTrue(indexDescriptor.isPerColumnIndexBuildComplete(plain.identifier()));
            assertTrue(indexDescriptor.isPerColumnIndexBuildComplete(analyzed.identifier()));
        }

        // '=' on the plain index is unaffected
        assertEquals(1, execute("SELECT id FROM %s WHERE v1 = 'apple'").size());
    }

    private void verifyAnalyzedSSTables(String indexName, int expectedSSTables)
    {
        StorageAttachedIndex index = analyzedIndex(indexName);
        assertEquals(expectedSSTables, getCurrentColumnFamilyStore().getLiveSSTables().size());
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            IndexDescriptor indexDescriptor = IndexDescriptor.create(sstable);
            assertTrue(indexDescriptor.isPerColumnIndexBuildComplete(index.identifier()));
            assertTrue(indexDescriptor.hasComponent(IndexComponent.POSITIONS, index.identifier()));
            assertTrue(indexDescriptor.validatePerIndexComponents(index.termType(), index.identifier(), IndexValidation.CHECKSUM, true, true));
        }
    }

    private long docCount(String indexName)
    {
        return sumPositionsAttribute(indexName, LiteralIndexWriter.DOC_COUNT);
    }

    private long sumDocLengths(String indexName)
    {
        return sumPositionsAttribute(indexName, LiteralIndexWriter.SUM_DOC_LENGTHS);
    }

    private long sumPositionsAttribute(String indexName, String attribute)
    {
        StorageAttachedIndex index = analyzedIndex(indexName);
        long sum = 0;
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            IndexDescriptor indexDescriptor = IndexDescriptor.create(sstable);
            for (SegmentMetadata segment : loadSegments(indexDescriptor, index))
                sum += Long.parseLong(segment.componentMetadatas.get(IndexComponent.POSITIONS).attributes.get(attribute));
        }
        return sum;
    }

    private List<String> flushedTerms(String indexName)
    {
        StorageAttachedIndex index = analyzedIndex(indexName);
        List<String> terms = new ArrayList<>();
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            IndexDescriptor indexDescriptor = IndexDescriptor.create(sstable);
            for (SegmentMetadata segment : loadSegments(indexDescriptor, index))
            {
                FileHandle termsData = indexDescriptor.createPerIndexFileHandle(IndexComponent.TERMS_DATA, index.identifier(), null);
                FileHandle postingLists = indexDescriptor.createPerIndexFileHandle(IndexComponent.POSTING_LISTS, index.identifier(), null);
                try (FileHandle ignored = termsData;
                     TermsScanner scanner = new TermsScanner(termsData, postingLists, segment.componentMetadatas.get(IndexComponent.TERMS_DATA).root))
                {
                    while (scanner.hasNext())
                    {
                        ByteComparable term = scanner.next().term;
                        byte[] bytes = ByteSourceInverse.readBytes(ByteSourceInverse.unescape(ByteSource.peekable(term.asComparableBytes(ByteComparable.Version.OSS50))));
                        terms.add(UTF8Type.instance.getString(ByteBuffer.wrap(bytes)));
                    }
                }
            }
        }
        return terms;
    }

    private List<SegmentMetadata> loadSegments(IndexDescriptor indexDescriptor, StorageAttachedIndex index)
    {
        try
        {
            MetadataSource source = MetadataSource.loadColumnMetadata(indexDescriptor, index.identifier());
            return SegmentMetadata.load(source, indexDescriptor.primaryKeyFactory);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private StorageAttachedIndex analyzedIndex(String indexName)
    {
        return (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.getIndexByName(indexName);
    }
}
