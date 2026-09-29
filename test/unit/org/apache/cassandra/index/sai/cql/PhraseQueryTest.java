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

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Session;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentBuilder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * The query matrix of the {@code PHRASE} operator: strict, gap-preserving adjacency on zero based
 * analyzer positions, intersected inside each segment and re-checked by positional re-analysis at
 * post-filter time.
 */
public class PhraseQueryTest extends SAITester
{
    @Before
    public void setup()
    {
        requireNetwork();
    }

    @Test
    public void adjacencyIsStrictAndOrdered() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'the quick brown fox')");
        execute("INSERT INTO %s (id, body) VALUES (2, 'the brown quick fox')");
        execute("INSERT INTO %s (id, body) VALUES (3, 'quick fox')");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body PHRASE 'quick brown'"), row(1));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body PHRASE 'brown fox'"), row(1));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body PHRASE 'quick brown fox'"), row(1));
            // order matters
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body PHRASE 'brown quick'"), row(2));
            // adjacency matters: row 1 has a token between quick and fox, rows 2 and 3 do not
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body PHRASE 'quick fox'"), row(2), row(3));
            // a single token phrase matches any occurrence
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body PHRASE 'quick'"), row(1), row(2), row(3));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body PHRASE 'wolf den'"));
        });

        compact();
        waitForCompactionsFinished();
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body PHRASE 'quick brown fox'"), row(1));
    }

    @Test
    public void stopwordGapIsPreserved() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'english' }");

        // The english analyzer indexes this as quick@0, fox@2, the stopword leaves a gap
        execute("INSERT INTO %s (id, body) VALUES (1, 'quick the fox')");
        execute("INSERT INTO %s (id, body) VALUES (2, 'quick fox')");

        beforeAndAfterFlush(() -> {
            // quick@0, fox@1 does not match quick@0, fox@2
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body PHRASE 'quick fox'"), row(2));
            // the query analyzed with the same gap does match
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body PHRASE 'quick the fox'"), row(1));
        });
    }

    @Test
    public void analyzerWithoutStopwordsKeepsContiguousPositions() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        // the standard analyzer removes nothing, so stopwords occupy real positions
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'quick the fox')");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body PHRASE 'quick the fox'"), row(1));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body PHRASE 'quick fox'"));
        });
    }

    @Test
    public void stopwordOnlyPhraseMatchesNothing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'english' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'the quick fox')");

        beforeAndAfterFlush(() -> assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body PHRASE 'the'")));
    }

    @Test
    public void repeatedTokensNeedRepeatedOccurrences() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'buffalo buffalo river')");
        execute("INSERT INTO %s (id, body) VALUES (2, 'buffalo river buffalo')");

        beforeAndAfterFlush(() -> assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body PHRASE 'buffalo buffalo'"), row(1)));
    }

    @Test
    public void multipleSSTablesAndMemtableCombine() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'quick brown fox')");
        flush();
        execute("INSERT INTO %s (id, body) VALUES (2, 'lazy quick brown dog')");
        flush();
        // this row stays in the memtable
        execute("INSERT INTO %s (id, body) VALUES (3, 'a quick brown bear')");

        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body PHRASE 'quick brown'"), row(1), row(2), row(3));

        compact();
        waitForCompactionsFinished();
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body PHRASE 'quick brown'"), row(1), row(2), row(3));
    }

    @Test
    public void multipleSegmentsPerSSTableCombine() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        for (int i = 0; i < 50; i++)
            execute("INSERT INTO %s (id, body) VALUES (?, ?)", i, i % 2 == 0 ? "quick brown fox " + i : "brown quick dog " + i);
        flush();

        try
        {
            // 7 rows per segment forces a multi-segment compaction build
            SegmentBuilder.updateLastValidSegmentRowId(7);
            compact();
            waitForCompactionsFinished();

            assertEquals(25, execute("SELECT id FROM %s WHERE body PHRASE 'quick brown fox'").size());
            assertEquals(25, execute("SELECT id FROM %s WHERE body PHRASE 'brown quick'").size());
            assertEquals(0, execute("SELECT id FROM %s WHERE body PHRASE 'fox quick'").size());
        }
        finally
        {
            SegmentBuilder.updateLastValidSegmentRowId(-1);
        }
    }

    @Test
    public void collectionsNeverMatchAcrossElements() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val list<text>)");
        createIndex("CREATE INDEX ON %s(val) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, val) VALUES (1, ['quick brown', 'fox jumps'])");
        execute("INSERT INTO %s (id, val) VALUES (2, ['quick brown fox'])");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE val PHRASE 'quick brown'"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE val PHRASE 'fox jumps'"), row(1));
            // 'brown' and 'fox' are adjacent only across an element boundary of row 1
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE val PHRASE 'brown fox'"), row(2));
        });
    }

    @Test
    public void updatedRowMatchesOnlyItsNewestValue() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'quick brown fox')");
        // the overwrite moves 'fox' next to 'quick'
        execute("INSERT INTO %s (id, body) VALUES (1, 'quick fox brown')");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body PHRASE 'quick fox'"), row(1));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body PHRASE 'quick brown'"));
        });
    }

    @Test
    public void phraseRequiresAnAnalyzedIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text, plain text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        createIndex("CREATE INDEX ON %s(plain) USING 'sai'");

        assertInvalidMessage("is only supported on columns with a storage-attached index using an index_analyzer",
                             "SELECT id FROM %s WHERE plain PHRASE 'quick fox'");
        assertInvalidMessage("is only supported on columns with a storage-attached index using an index_analyzer",
                             "SELECT id FROM %s WHERE plain PHRASE 'quick fox' ALLOW FILTERING");
    }

    @Test
    public void phraseRejectedOutsideSelect() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        assertInvalidMessage("Cannot use UPDATE with PHRASE",
                             "UPDATE %s SET body = 'x' WHERE body PHRASE 'quick fox'");
        assertInvalidMessage("Cannot use DELETE with PHRASE",
                             "DELETE FROM %s WHERE body PHRASE 'quick fox'");
    }

    @Test
    public void tracingReportsPhraseIntersection() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'quick brown fox')");
        execute("INSERT INTO %s (id, body) VALUES (2, 'brown quick dog')");
        flush();

        Session session = sessionNet();
        String analysisTrace = getSingleTraceStatement(session, "SELECT id FROM %s WHERE body PHRASE 'quick brown'", "Query analyzed");
        assertNotNull(analysisTrace);
        assertTrue(analysisTrace, analysisTrace.contains("quick@0"));
        assertTrue(analysisTrace, analysisTrace.contains("brown@1"));

        String intersectionTrace = getSingleTraceStatement(session, "SELECT id FROM %s WHERE body PHRASE 'quick brown'", "Phrase intersection");
        assertNotNull(intersectionTrace);
        assertTrue(intersectionTrace, intersectionTrace.contains("matched 1 of 2 candidates for 2 tokens"));
    }

    @Test
    public void tracingReportsMemtablePhraseIntersection() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'quick brown fox')");
        execute("INSERT INTO %s (id, body) VALUES (2, 'brown quick dog')");

        Session session = sessionNet();
        String trace = getSingleTraceStatement(session, "SELECT id FROM %s WHERE body PHRASE 'quick brown'", "Phrase intersection on memtable");
        assertNotNull(trace);
        assertTrue(trace, trace.contains("matched 1 of 2 candidates for 2 tokens"));
    }

    @Test
    public void severalPhrasesOnACollectionMatchWithinElements() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val list<text>)");
        createIndex("CREATE INDEX ON %s(val) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, val) VALUES (1, ['quick brown', 'fox jumps'])");
        execute("INSERT INTO %s (id, val) VALUES (2, ['quick brown fox'])");

        beforeAndAfterFlush(() -> {
            // each phrase sits inside one element, and each relation may use a different element
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE val PHRASE 'quick brown' AND val PHRASE 'fox jumps'"), row(1));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE val PHRASE 'quick brown' AND val PHRASE 'brown fox'"), row(2));
        });
    }
}
