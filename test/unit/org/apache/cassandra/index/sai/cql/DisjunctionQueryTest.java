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

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * The OR query matrix on SAI indexed columns: unions of index results per OR node, AND branches
 * under OR, analyzed leaves under OR, and the ALLOW FILTERING fallback for unindexed leaves.
 */
public class DisjunctionQueryTest extends SAITester
{
    @Before
    public void setup()
    {
        requireNetwork();
    }

    @Test
    public void basicDisjunctionAcrossMemtableAndSSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, a int, b int)");
        createIndex("CREATE INDEX ON %s(a) USING 'sai'");
        createIndex("CREATE INDEX ON %s(b) USING 'sai'");

        execute("INSERT INTO %s (pk, a, b) VALUES (1, 1, 9)");
        execute("INSERT INTO %s (pk, a, b) VALUES (2, 9, 2)");
        execute("INSERT INTO %s (pk, a, b) VALUES (3, 9, 9)");
        execute("INSERT INTO %s (pk, a, b) VALUES (4, 1, 2)");

        // Fully indexed disjunctions need no ALLOW FILTERING
        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE a = 1 OR b = 2"),
                                    row(1), row(2), row(4));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE a = 1 OR a = 9"),
                                    row(1), row(2), row(3), row(4));
        });

        compact();
        waitForCompactionsFinished();
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE a = 1 OR b = 2"),
                                row(1), row(2), row(4));

        // Mixed memtable and sstable data
        execute("INSERT INTO %s (pk, a, b) VALUES (5, 1, 9)");
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE a = 1 OR b = 2"),
                                row(1), row(2), row(4), row(5));
    }

    @Test
    public void nestedBooleanShapes() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, a int, b int, c int, d int)");
        createIndex("CREATE INDEX ON %s(a) USING 'sai'");
        createIndex("CREATE INDEX ON %s(b) USING 'sai'");
        createIndex("CREATE INDEX ON %s(c) USING 'sai'");
        createIndex("CREATE INDEX ON %s(d) USING 'sai'");

        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (1, 1, 2, 9, 9)");
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (2, 1, 9, 9, 9)");
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (3, 9, 9, 3, 9)");
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (4, 9, 9, 3, 4)");

        beforeAndAfterFlush(() -> {
            // AND under OR
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE (a = 1 AND b = 2) OR c = 3"),
                                    row(1), row(3), row(4));
            // OR under AND
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE a = 1 AND (b = 2 OR c = 9)"),
                                    row(1), row(2));
            // two AND branches
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE (a = 1 AND b = 2) OR (c = 3 AND d = 4)"),
                                    row(1), row(4));
            // three way union
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE a = 1 OR b = 2 OR d = 4"),
                                    row(1), row(2), row(4));
        });
    }

    @Test
    public void sameColumnRangesNeverFoldAcrossBranches() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val int)");
        createIndex("CREATE INDEX ON %s(val) USING 'sai'");

        for (int i = 0; i < 20; i++)
            execute("INSERT INTO %s (pk, val) VALUES (?, ?)", i, i);

        // val < 5 and val > 10 folded into one range would produce an empty match
        beforeAndAfterFlush(() ->
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE val < 2 OR val > 17"),
                                    row(0), row(1), row(18), row(19)));

        // a branch with both bounds keeps them conjoined
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE (val > 3 AND val < 6) OR val = 15"),
                                row(4), row(5), row(15));
    }

    @Test
    public void containsUnderDisjunction() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, tags set<text>, v int)");
        createIndex("CREATE INDEX ON %s(tags) USING 'sai'");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        execute("INSERT INTO %s (pk, tags, v) VALUES (1, {'red', 'blue'}, 9)");
        execute("INSERT INTO %s (pk, tags, v) VALUES (2, {'green'}, 9)");
        execute("INSERT INTO %s (pk, tags, v) VALUES (3, {'yellow'}, 7)");

        beforeAndAfterFlush(() ->
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE tags CONTAINS 'red' OR v = 7"),
                                    row(1), row(3)));
    }

    @Test
    public void analyzedMatchUnderDisjunction() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, body text, v int)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        execute("INSERT INTO %s (pk, body, v) VALUES (1, 'The quick brown fox', 9)");
        execute("INSERT INTO %s (pk, body, v) VALUES (2, 'The lazy brown dog', 9)");
        execute("INSERT INTO %s (pk, body, v) VALUES (3, 'A quick dog', 7)");

        beforeAndAfterFlush(() -> {
            // The multi token leaf keeps AND semantics inside its branch of the union
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE body : 'quick brown' OR v = 7"),
                                    row(1), row(3));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE body : 'quick brown' OR body : 'lazy'"),
                                    row(1), row(2));
            // tokens that only match across branches must not leak into one another
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE body : 'lazy fox' OR v = 7"),
                                    row(3));
        });
    }

    @Test
    public void phraseUnderDisjunction() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, body text, v int)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        execute("INSERT INTO %s (pk, body, v) VALUES (1, 'The quick brown fox', 9)");
        execute("INSERT INTO %s (pk, body, v) VALUES (2, 'The brown quick fox', 9)");
        execute("INSERT INTO %s (pk, body, v) VALUES (3, 'A quick dog', 7)");

        beforeAndAfterFlush(() ->
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE body PHRASE 'quick brown' OR v = 7"),
                                    row(1), row(3)));
    }

    @Test
    public void unindexedLeavesRequireAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, a int, b int)");
        createIndex("CREATE INDEX ON %s(a) USING 'sai'");

        execute("INSERT INTO %s (pk, a, b) VALUES (1, 1, 9)");
        execute("INSERT INTO %s (pk, a, b) VALUES (2, 9, 2)");

        // A disjunction with an unindexed leaf must not use the index, the unindexed branch
        // would contribute nothing to the union
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT pk FROM %s WHERE a = 1 OR b = 2");

        beforeAndAfterFlush(() ->
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE a = 1 OR b = 2 ALLOW FILTERING"),
                                    row(1), row(2)));
    }

    @Test
    public void disjunctionCrossingIntersectionClauseLimit() throws Throwable
    {
        // The intersection clause limit defaults to two ranges per intersection, a three clause
        // AND branch under OR relies on the post-filter to recover the dropped clause
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, a int, b int, c int, d int)");
        createIndex("CREATE INDEX ON %s(a) USING 'sai'");
        createIndex("CREATE INDEX ON %s(b) USING 'sai'");
        createIndex("CREATE INDEX ON %s(c) USING 'sai'");
        createIndex("CREATE INDEX ON %s(d) USING 'sai'");

        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (1, 1, 1, 1, 9)");
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (2, 1, 1, 9, 9)");
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (3, 9, 9, 9, 4)");

        beforeAndAfterFlush(() ->
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE (a = 1 AND b = 1 AND c = 1) OR d = 4"),
                                    row(1), row(3)));
    }

    @Test
    public void pagingOverDisjunction() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, a int, b int)");
        createIndex("CREATE INDEX ON %s(a) USING 'sai'");
        createIndex("CREATE INDEX ON %s(b) USING 'sai'");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (pk, a, b) VALUES (?, ?, ?)", i, i % 2, i % 3);
        flush();

        // a = 0 matches pk 0, 2, 4, 6, 8 and b = 1 matches pk 1, 4, 7
        for (int pageSize : new int[]{ 1, 2, 3, 100 })
        {
            Set<Integer> results = new HashSet<>();
            for (Row fetched : executeNetWithPaging("SELECT pk FROM %s WHERE a = 0 OR b = 1", pageSize))
                assertTrue("Duplicate row across pages for page size " + pageSize, results.add(fetched.getInt("pk")));
            assertEquals("Unexpected rows for page size " + pageSize, Set.of(0, 1, 2, 4, 6, 7, 8), results);
        }
    }

    @Test
    public void readCommandRendersClusteringConjunctWithDisjunction() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(a) USING 'sai'");
        createIndex("CREATE INDEX ON %s(b) USING 'sai'");

        execute("INSERT INTO %s (pk, ck, a, b) VALUES (0, 6, 1, 9)");

        String query = "SELECT * FROM %s WHERE pk = 0 AND ck > 5 AND (a = 1 OR b = 2) ALLOW FILTERING";
        assertRowsIgnoringOrder(execute(query), row(0, 6, 1, 9));

        // The CQL rendering drops root filter expressions duplicated by the clustering filter,
        // which must tolerate a root holding only a disjunction
        SelectStatement select = (SelectStatement) QueryProcessor.getStatement(formatQuery(query), ClientState.forInternalCalls());
        ReadCommand command = (ReadCommand) select.getQuery(QueryOptions.DEFAULT, FBUtilities.nowInSeconds());
        assertTrue(command.toCQLString(), command.toCQLString().contains(" OR "));
    }

    @Test
    public void staticOnlyPartitionParityWithFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck))");
        String sIndex = createIndex("CREATE INDEX ON %s(s) USING 'sai'");
        String vIndex = createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        execute("INSERT INTO %s (pk, s) VALUES (1, 1)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, 1, 2)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (3, 1, 9)");
        execute("INSERT INTO %s (pk, s) VALUES (3, 1)");
        flush();

        // A partition whose only content is a matching static row produces no row for a query
        // restricted on regular columns, on the index path...
        assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE s = 1 OR v = 2"),
                                row(2, 1), row(3, 1));

        // ...and identically on the filtering path
        dropIndex("DROP INDEX %s." + sIndex);
        dropIndex("DROP INDEX %s." + vIndex);
        assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE s = 1 OR v = 2 ALLOW FILTERING"),
                                row(2, 1), row(3, 1));
    }

    @Test
    public void tracingReportsTreeShape() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, a int, b int, c int)");
        createIndex("CREATE INDEX ON %s(a) USING 'sai'");
        createIndex("CREATE INDEX ON %s(b) USING 'sai'");
        createIndex("CREATE INDEX ON %s(c) USING 'sai'");

        execute("INSERT INTO %s (pk, a, b, c) VALUES (1, 1, 2, 3)");
        flush();

        Session session = sessionNet();
        String trace = getSingleTraceStatement(session,
                                               "SELECT pk FROM %s WHERE (a = 1 AND b = 2) OR c = 3",
                                               "Executing index query tree");
        assertNotNull(trace);
        assertTrue(trace, trace.contains("OrNode"));
        assertTrue(trace, trace.contains("AndNode"));
    }
}
