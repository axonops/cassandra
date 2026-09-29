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
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * The query matrix of {@code MATCH KEY} and {@code PHRASE KEY}: word search over the keys of a
 * non-frozen map through an analyzed index on {@code KEYS(column)}, next to {@code MATCH} and
 * {@code PHRASE} over the values through an analyzed index on {@code VALUES(column)}.
 */
public class MapKeysAnalyzedQueryTest extends SAITester
{
    private static final String NEEDS_NON_FROZEN_MAP = "MATCH KEY and PHRASE KEY need a non-frozen map column, %s is not one";

    @Before
    public void setup()
    {
        requireNetwork();
    }

    @Test
    public void keyOperatorsSearchKeysAndValueOperatorsSearchValues() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, attrs map<text, text>)");
        createIndex("CREATE INDEX ON %s(KEYS(attrs)) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        createIndex("CREATE INDEX ON %s(VALUES(attrs)) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (pk, attrs) VALUES (1, {'error code': 'disk full', 'owner': 'ops team'})");
        execute("INSERT INTO %s (pk, attrs) VALUES (2, {'warning': 'error code seen'})");

        beforeAndAfterFlush(this::assertKeysAndValuesMatrix);

        compact();
        waitForCompactionsFinished();
        assertKeysAndValuesMatrix();
    }

    private void assertKeysAndValuesMatrix() throws Throwable
    {
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs MATCH KEY 'error'"), row(1));
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs MATCH KEY 'code error'"), row(1));
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs MATCH KEY 'disk'"));
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs PHRASE KEY 'error code'"), row(1));
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs PHRASE KEY 'code error'"));

        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs MATCH 'error'"), row(2));
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs MATCH 'disk'"), row(1));
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs PHRASE 'error code'"), row(2));
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs PHRASE 'owner'"));
    }

    @Test
    public void phraseKeyNeverMatchesAcrossKeys() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, attrs map<text, text>, u int)");
        createIndex("CREATE INDEX ON %s(KEYS(attrs)) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        // Keys are stored in order, so 'error' ends the first key and 'code' starts the next one
        execute("INSERT INTO %s (pk, attrs, u) VALUES (1, {'alpha error': 'x', 'code beta': 'y'}, 0)");

        Session session = sessionNet();
        String memtableTrace = getSingleTraceStatement(session, "SELECT pk FROM %s WHERE attrs PHRASE KEY 'error code'",
                                                       "Phrase intersection on memtable");
        assertNotNull(memtableTrace);
        assertTrue(memtableTrace, memtableTrace.contains("matched 0 of 1 candidates for 2 tokens"));

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs PHRASE KEY 'error code'"));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs PHRASE KEY 'alpha error'"), row(1));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs MATCH KEY 'error code'"), row(1));
            // u has no index, so the row filter evaluates the phrase key by key
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs PHRASE KEY 'error code' OR u = 5 ALLOW FILTERING"));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs PHRASE KEY 'alpha error' OR u = 5 ALLOW FILTERING"), row(1));
        });

        String sstableTrace = getSingleTraceStatement(session, "SELECT pk FROM %s WHERE attrs PHRASE KEY 'error code'",
                                                      "Phrase intersection on index");
        assertNotNull(sstableTrace);
        assertTrue(sstableTrace, sstableTrace.contains("matched 0 of 1 candidates for 2 tokens"));
    }

    @Test
    public void keysAndValuesUseTheirOwnAnalyzers() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, attrs map<text, text>)");
        createIndex("CREATE INDEX ON %s(KEYS(attrs)) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'english' }");
        createIndex("CREATE INDEX ON %s(VALUES(attrs)) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (pk, attrs) VALUES (1, {'running': 'running'})");

        beforeAndAfterFlush(() -> {
            // english stems runs and running to run, standard keeps them apart
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs MATCH KEY 'runs'"), row(1));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs MATCH 'runs'"));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs MATCH 'running'"), row(1));
        });

        // The row filter re-analysis, used on the coordinator and on the filtering path, looks up
        // the analyzer of the index on the searched target
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        ColumnMetadata attrs = cfs.metadata().getColumn(ColumnIdentifier.getInterned("attrs", false));
        DecoratedKey key = Util.dk(Int32Type.instance.decompose(1));
        Row row = Util.getOnlyRow(Util.cmd(cfs, 1).build());
        long nowInSec = FBUtilities.nowInSeconds();

        RowFilter matchValues = RowFilter.create(false);
        matchValues.add(attrs, Operator.ANALYZER_MATCHES, UTF8Type.instance.decompose("runs"));
        assertFalse(matchValues.isSatisfiedBy(cfs.metadata(), key, row, nowInSec));

        RowFilter matchKeys = RowFilter.create(false);
        matchKeys.add(attrs, Operator.ANALYZER_MATCHES_KEY, UTF8Type.instance.decompose("runs"));
        assertTrue(matchKeys.isSatisfiedBy(cfs.metadata(), key, row, nowInSec));
    }

    @Test
    public void containsKeyComparesWholeKeys() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, attrs map<text, text>, tags map<text, text>)");
        createIndex("CREATE INDEX ON %s(KEYS(attrs)) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        createIndex("CREATE INDEX ON %s(KEYS(tags)) USING 'sai'");

        execute("INSERT INTO %s (pk, attrs, tags) VALUES (1, {'error code': 'x'}, {'error code': 'y'})");

        assertInvalidMessage(String.format(StatementRestrictions.ANALYZED_CONTAINS_KEY_MESSAGE, "attrs"),
                             "SELECT pk FROM %s WHERE attrs CONTAINS KEY 'error code'");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE tags CONTAINS KEY 'error code'"), row(1));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE tags CONTAINS KEY 'error'"));
        });
    }

    @Test
    public void keyOperatorsNeedANonFrozenMap() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, l list<text>, t text, f frozen<map<text, text>>)");
        createIndex("CREATE INDEX ON %s(l) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        createIndex("CREATE INDEX ON %s(t) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        assertInvalidMessage(String.format(NEEDS_NON_FROZEN_MAP, "l"), "SELECT pk FROM %s WHERE l MATCH KEY 'x'");
        assertInvalidMessage(String.format(NEEDS_NON_FROZEN_MAP, "t"), "SELECT pk FROM %s WHERE t MATCH KEY 'x'");
        assertInvalidMessage(String.format(NEEDS_NON_FROZEN_MAP, "t"), "SELECT pk FROM %s WHERE t PHRASE KEY 'x'");
        assertInvalidMessage(String.format(NEEDS_NON_FROZEN_MAP, "f"), "SELECT pk FROM %s WHERE f MATCH KEY 'x' ALLOW FILTERING");
    }

    @Test
    public void keyOperatorsNameTheMissingIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, attrs map<text, text>, plain map<text, text>, keyed map<text, text>, rawkeys map<text, text>)");
        createIndex("CREATE INDEX ON %s(VALUES(attrs)) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        createIndex("CREATE INDEX ON %s(KEYS(keyed)) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        createIndex("CREATE INDEX ON %s(KEYS(rawkeys)) USING 'sai'");

        // an analyzed index on the values does not serve the key operators
        assertInvalidMessage(String.format(StatementRestrictions.MAP_KEYS_OPERATOR_REQUIRES_INDEX_MESSAGE, "MATCH KEY", "attrs"),
                             "SELECT pk FROM %s WHERE attrs MATCH KEY 'x'");
        assertInvalidMessage(String.format(StatementRestrictions.MAP_KEYS_OPERATOR_REQUIRES_INDEX_MESSAGE, "PHRASE KEY", "attrs"),
                             "SELECT pk FROM %s WHERE attrs PHRASE KEY 'x'");
        assertInvalidMessage(String.format(StatementRestrictions.MAP_KEYS_OPERATOR_REQUIRES_INDEX_MESSAGE, "MATCH KEY", "plain"),
                             "SELECT pk FROM %s WHERE plain MATCH KEY 'x' ALLOW FILTERING");
        // a KEYS index without an index_analyzer does not serve them either
        assertInvalidMessage(String.format(StatementRestrictions.MAP_KEYS_OPERATOR_REQUIRES_INDEX_MESSAGE, "MATCH KEY", "rawkeys"),
                             "SELECT pk FROM %s WHERE rawkeys MATCH KEY 'x'");

        // and an analyzed index on the keys does not serve the value operators
        assertInvalidMessage(String.format(StatementRestrictions.ANALYZED_OPERATOR_REQUIRES_INDEX_MESSAGE, "MATCH", "keyed MATCH 'x'"),
                             "SELECT pk FROM %s WHERE keyed MATCH 'x'");
        assertInvalidMessage(String.format(StatementRestrictions.ANALYZED_OPERATOR_REQUIRES_INDEX_MESSAGE, "PHRASE", "keyed PHRASE 'x'"),
                             "SELECT pk FROM %s WHERE keyed PHRASE 'x'");
    }

    @Test
    public void matchTakesTokensFromAnyElementOrKey() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, s set<text>, l list<text>, attrs map<text, text>, u int)");
        createIndex("CREATE INDEX ON %s(s) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        createIndex("CREATE INDEX ON %s(l) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        createIndex("CREATE INDEX ON %s(KEYS(attrs)) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        createIndex("CREATE INDEX ON %s(VALUES(attrs)) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        // 'owner' and 'error' always sit in two different elements or keys of row 1
        execute("INSERT INTO %s (pk, s, l, attrs, u) VALUES (1, {'owner', 'error code'}, ['owner', 'error code'], " +
                "{'owner': 'disk', 'error code': 'full'}, 0)");
        execute("INSERT INTO %s (pk, s, l, attrs, u) VALUES (2, {'other'}, ['other'], {'other': 'other'}, 5)");

        beforeAndAfterFlush(() -> {
            // the index path
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE s MATCH 'owner error'"), row(1));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE l MATCH 'owner error'"), row(1));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs MATCH 'disk full'"), row(1));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs MATCH KEY 'owner error'"), row(1));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE s PHRASE 'owner error'"));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs PHRASE KEY 'owner error'"));

            // u has no index, so these run on the filtering path with the row filter re-analysis
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE s MATCH 'owner error' OR u = 5 ALLOW FILTERING"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE l MATCH 'owner error' OR u = 5 ALLOW FILTERING"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs MATCH 'disk full' OR u = 5 ALLOW FILTERING"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs MATCH KEY 'owner error' OR u = 5 ALLOW FILTERING"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE s PHRASE 'owner error' OR u = 5 ALLOW FILTERING"), row(2));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs PHRASE KEY 'owner error' OR u = 5 ALLOW FILTERING"), row(2));
        });
    }

    @Test
    public void stopwordOnlyKeyQueryMatchesNothing() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, attrs map<text, text>)");
        createIndex("CREATE INDEX ON %s(KEYS(attrs)) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'english' }");

        execute("INSERT INTO %s (pk, attrs) VALUES (1, {'the quick fox': 'x'})");
        execute("INSERT INTO %s (pk, attrs) VALUES (2, {'the and of': 'y'})");

        beforeAndAfterFlush(() -> {
            // 'the' analyzes to no tokens, so it matches nothing, not everything
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs MATCH KEY 'the'"));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs MATCH KEY 'the of and'"));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs PHRASE KEY 'the'"));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs PHRASE KEY 'the of and'"));
            // a stopword next to a real token does not change the real token's match
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs MATCH KEY 'the quick'"), row(1));
        });
    }

    @Test
    public void matchKeyInsideOr() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, attrs map<text, text>, v int, u int)");
        createIndex("CREATE INDEX ON %s(KEYS(attrs)) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        execute("INSERT INTO %s (pk, attrs, v, u) VALUES (1, {'error code': 'x'}, 0, 0)");
        execute("INSERT INTO %s (pk, attrs, v, u) VALUES (2, {'owner': 'error'}, 7, 5)");
        execute("INSERT INTO %s (pk, attrs, v, u) VALUES (3, {'owner': 'error'}, 0, 0)");

        beforeAndAfterFlush(() -> {
            // every branch indexed, the index path
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs MATCH KEY 'error' OR v = 7"), row(1), row(2));
            // u has no index, so the query runs on the filtering path with the row filter re-analysis
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs MATCH KEY 'error' OR u = 5 ALLOW FILTERING"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE attrs PHRASE KEY 'error code' OR u = 5 ALLOW FILTERING"), row(1), row(2));
        });
    }

    @Test
    public void keyOperatorsRenderWithTheKeyType() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, scores map<text, int>)");
        createIndex("CREATE INDEX ON %s(KEYS(scores)) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (pk, scores) VALUES (1, {'error code': 5})");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        ColumnMetadata scores = cfs.metadata().getColumn(ColumnIdentifier.getInterned("scores", false));
        RowFilter filter = RowFilter.create(false);
        filter.add(scores, Operator.ANALYZER_MATCHES_KEY, UTF8Type.instance.decompose("error"));
        assertEquals("scores MATCH KEY 'error'", filter.toCQLString());

        Session session = sessionNet();
        String trace = getSingleTraceStatement(session, "SELECT pk FROM %s WHERE scores MATCH KEY 'error code'", "Query analyzed");
        assertNotNull(trace);
        assertTrue(trace, trace.contains("error@0"));
        assertRows(execute("SELECT pk FROM %s WHERE scores MATCH KEY 'error'"), row(1));
    }
}
