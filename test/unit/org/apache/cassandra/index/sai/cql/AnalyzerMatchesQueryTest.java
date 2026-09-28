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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * The query matrix of the {@code MATCH} operator: AND of query analyzer tokens, re-checked by
 * re-analysis with the index analyzer at post-filter time.
 */
public class AnalyzerMatchesQueryTest extends SAITester
{
    @Before
    public void setup()
    {
        requireNetwork();
    }

    @Test
    public void singleAndMultiTokenAndSemantics() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'The quick brown fox')");
        execute("INSERT INTO %s (id, body) VALUES (2, 'The lazy brown dog')");
        execute("INSERT INTO %s (id, body) VALUES (3, 'A quick dog')");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'quick'"), row(1), row(3));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'brown'"), row(1), row(2));
            // multi token means AND: order and distance do not matter
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'quick brown'"), row(1));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'fox quick'"), row(1));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'dog quick brown'"));
            // the standard analyzer lower cases, so query case is irrelevant
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'QUICK'"), row(1), row(3));
            // no row has this token
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'wolf'"));
        });

        compact();
        waitForCompactionsFinished();
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'quick brown'"), row(1));
    }

    @Test
    public void duplicateQueryTokensBehaveLikeOne() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'quick fox')");

        beforeAndAfterFlush(() -> assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'quick quick'"), row(1)));
    }

    @Test
    public void stopwordOnlyQueryMatchesNothingConsistently() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'english' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'the quick fox')");
        execute("INSERT INTO %s (id, body) VALUES (2, 'the and of')");

        beforeAndAfterFlush(() -> {
            // 'the' analyzes to no tokens, so it matches nothing, not everything
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'the'"));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'the of and'"));
            // a stopword next to a real token does not change the real token's match
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'the quick'"), row(1));
        });
    }

    @Test
    public void emptyQueryStringMatchesNothing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'quick fox')");
        execute("INSERT INTO %s (id, body) VALUES (2, '')");

        beforeAndAfterFlush(() -> assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH ''")));
    }

    @Test
    public void queryAnalyzerAppliesToQueriesOnly() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        // The index analyzer stems, the query analyzer does not
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = " +
                    "{ 'index_analyzer' : 'english', 'query_analyzer' : 'whitespace' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'running foxes')");

        beforeAndAfterFlush(() -> {
            // the index analyzer stemmed the stored value to 'run', 'fox'; the whitespace query
            // analyzer leaves the query tokens alone
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'run'"), row(1));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'run fox'"), row(1));
            // an unstemmed query token was never indexed
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'running'"));
        });
    }

    @Test
    public void postFilterReanalyzesWithTheIndexAnalyzer() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        // Index analyzer stems 'running' to 'run'; the query analyzer emits 'run' unchanged. The
        // post-filter must re-analyze the stored value with the INDEX analyzer: re-analysis with
        // the query analyzer would emit 'running' and wrongly drop the row.
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = " +
                    "{ 'index_analyzer' : 'english', 'query_analyzer' : 'whitespace' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'running fast')");

        beforeAndAfterFlush(() -> assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'run'"), row(1)));
    }

    @Test
    public void collectionsMatchTokensAcrossElements() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val list<text>)");
        createIndex("CREATE INDEX ON %s(val) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, val) VALUES (1, ['quick brown', 'lazy dog'])");
        execute("INSERT INTO %s (id, val) VALUES (2, ['slow dog'])");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE val MATCH 'dog'"), row(1), row(2));
            // AND of tokens, each token may come from any element
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE val MATCH 'quick dog'"), row(1));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE val MATCH 'quick slow'"));
        });
    }

    @Test
    public void containsIsAnalyzedOnCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val set<text>)");
        createIndex("CREATE INDEX ON %s(val) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, val) VALUES (1, {'quick brown fox'})");
        execute("INSERT INTO %s (id, val) VALUES (2, {'lazy brown dog'})");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE val CONTAINS 'brown'"), row(1), row(2));
            // CONTAINS analyzes its value too: multiple tokens are an AND
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE val CONTAINS 'quick fox'"), row(1));
        });
    }

    @Test
    public void updatedRowMatchesOnlyItsNewestValue() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'quick fox')");
        execute("INSERT INTO %s (id, body) VALUES (1, 'lazy dog')");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'quick'"));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'lazy dog'"), row(1));
        });
    }

    @Test
    public void matchOperatorRequiresAnAnalyzedIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text, plain text, unindexed text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        createIndex("CREATE INDEX ON %s(plain) USING 'sai'");

        // no index at all on the column
        assertInvalidMessage("is only supported on columns with a storage-attached index using an index_analyzer",
                             "SELECT id FROM %s WHERE unindexed MATCH 'quick' ALLOW FILTERING");
        // a SAI index without an analyzer does not support the operator
        assertInvalidMessage("is only supported on columns with a storage-attached index using an index_analyzer",
                             "SELECT id FROM %s WHERE plain MATCH 'quick'");
        assertInvalidMessage("is only supported on columns with a storage-attached index using an index_analyzer",
                             "SELECT id FROM %s WHERE plain MATCH 'quick' ALLOW FILTERING");
    }

    @Test
    public void matchOperatorRejectedOutsideSelect() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        assertInvalidMessage("Cannot use UPDATE with MATCH",
                             "UPDATE %s SET body = 'x' WHERE body MATCH 'quick'");
        assertInvalidMessage("Cannot use DELETE with MATCH",
                             "DELETE FROM %s WHERE body MATCH 'quick'");
        assertInvalidSyntax("UPDATE %s SET body = 'x' WHERE id = 1 IF body MATCH 'quick'");
    }

    @Test
    public void matchOperatorRejectsUnsupportedRelationShapes() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, c1 int, c2 int, body text, m map<text, text>)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        createIndex("CREATE INDEX ON %s(m) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        assertInvalidSyntax("SELECT id FROM %s WHERE (c1, c2) MATCH (1, 2) ALLOW FILTERING");
        assertInvalidSyntax("SELECT id FROM %s WHERE token(id) MATCH 'quick' ALLOW FILTERING");
        assertInvalidSyntax("SELECT id FROM %s WHERE m['key'] MATCH 'quick'");
    }

    @Test
    public void matchOperatorRejectsMultipleRelationsOnOneColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        assertInvalidMessage("cannot be restricted by more than one relation",
                             "SELECT id FROM %s WHERE body MATCH 'quick' AND body MATCH 'fox'");
    }

    @Test
    public void tracingReportsQueryAnalysis() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'english' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'the quick foxes')");
        flush();

        Session session = sessionNet();
        String trace = getSingleTraceStatement(session, "SELECT id FROM %s WHERE body MATCH 'the quick foxes'", "Query analyzed");
        assertNotNull(trace);
        assertTrue(trace, trace.contains("quick@1"));
        assertTrue(trace, trace.contains("fox@2"));

        String postFilterTrace = getSingleTraceStatement(session, "SELECT id FROM %s WHERE body MATCH 'quick'", "Index post-filter matched");
        assertNotNull(postFilterTrace);
        assertEquals("Index post-filter matched 1 of 1 rows", postFilterTrace);
    }
}
