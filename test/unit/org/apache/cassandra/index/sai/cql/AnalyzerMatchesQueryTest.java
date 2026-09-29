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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
    public void containsIsRefusedOnAnalyzedCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val set<text>)");
        createIndex("CREATE INDEX ON %s(val) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, val) VALUES (1, {'quick brown fox'})");
        execute("INSERT INTO %s (id, val) VALUES (2, {'lazy brown dog'})");

        beforeAndAfterFlush(() -> {
            String refused = String.format(StatementRestrictions.ANALYZED_CONTAINS_MESSAGE, "val");
            assertInvalidMessage(refused, "SELECT id FROM %s WHERE val CONTAINS 'brown'");
            assertInvalidMessage(refused, "SELECT id FROM %s WHERE val CONTAINS 'quick fox'");
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE val MATCH 'quick fox'"), row(1));
        });
    }

    @Test
    public void matchPostFilteredBehindALegacyIndexTakesTokensFromAnyElement() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val set<text>, other int)");
        createIndex("CREATE INDEX ON %s(val) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        createIndex("CREATE INDEX ON %s(other) USING 'legacy_local_table'");

        execute("INSERT INTO %s (id, val, other) VALUES (1, {'quick fox', 'lazy dog'}, 5)");
        execute("INSERT INTO %s (id, val, other) VALUES (2, {'quick fox'}, 5)");
        execute("INSERT INTO %s (id, val, other) VALUES (3, {'quick fox', 'lazy dog'}, 6)");

        // Two index implementations need ALLOW FILTERING. The legacy index plan wins by default, so
        // the row filter re-checks MATCH on its rows. 'quick' and 'dog' come from two different
        // elements of row 1.
        beforeAndAfterFlush(() -> assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE val MATCH 'quick dog' AND other = 5 ALLOW FILTERING"), row(1)));
    }

    @Test
    public void containsFiltersAreNotPlannedOnAnalyzedIndexes() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val set<text>, m map<text, text>)");
        createIndex("CREATE INDEX ON %s(val) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        createIndex("CREATE INDEX ON %s(KEYS(m)) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        ColumnMetadata val = cfs.metadata().getColumn(ColumnIdentifier.getInterned("val", false));
        ColumnMetadata m = cfs.metadata().getColumn(ColumnIdentifier.getInterned("m", false));

        // built directly, so the statement layer refusal is not involved
        RowFilter contains = RowFilter.create(false);
        contains.add(val, Operator.CONTAINS, UTF8Type.instance.decompose("quick"));
        assertNull(cfs.indexManager.getBestIndexQueryPlanFor(contains));

        RowFilter containsKey = RowFilter.create(false);
        containsKey.add(m, Operator.CONTAINS_KEY, UTF8Type.instance.decompose("colour"));
        assertNull(cfs.indexManager.getBestIndexQueryPlanFor(containsKey));

        RowFilter match = RowFilter.create(false);
        match.add(val, Operator.ANALYZER_MATCHES, UTF8Type.instance.decompose("quick"));
        assertNotNull(cfs.indexManager.getBestIndexQueryPlanFor(match));

        RowFilter matchKey = RowFilter.create(false);
        matchKey.add(m, Operator.ANALYZER_MATCHES_KEY, UTF8Type.instance.decompose("colour"));
        assertNotNull(cfs.indexManager.getBestIndexQueryPlanFor(matchKey));
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

        createTable("CREATE TABLE %s (id int PRIMARY KEY, attrs map<text, text>)");
        createIndex("CREATE INDEX ON %s(KEYS(attrs)) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        assertInvalidMessage("Cannot use UPDATE with MATCH KEY",
                             "UPDATE %s SET attrs = {} WHERE attrs MATCH KEY 'quick'");
        assertInvalidMessage("Cannot use DELETE with PHRASE KEY",
                             "DELETE FROM %s WHERE attrs PHRASE KEY 'quick'");
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
    public void severalAnalyzedRelationsOnOneColumnAllMatch() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'the quick brown fox')");
        execute("INSERT INTO %s (id, body) VALUES (2, 'quick fox brown')");
        execute("INSERT INTO %s (id, body) VALUES (3, 'lazy dog')");

        beforeAndAfterFlush(this::assertSeveralAnalyzedRelations);

        compact();
        waitForCompactionsFinished();
        assertSeveralAnalyzedRelations();

        // On a static column every row of the matching partition is returned
        createTable("CREATE TABLE %s (pk int, ck int, s text static, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(s) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (pk, ck, s) VALUES (1, 1, 'the quick brown fox')");
        execute("INSERT INTO %s (pk, ck) VALUES (1, 2)");
        execute("INSERT INTO %s (pk, ck, s) VALUES (2, 1, 'quick fox brown')");

        beforeAndAfterFlush(() -> assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE s MATCH 'quick' AND s PHRASE 'brown fox'"),
                                                          row(1, 1), row(1, 2)));
    }

    private void assertSeveralAnalyzedRelations() throws Throwable
    {
        // every relation must match
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'quick' AND body MATCH 'fox'"), row(1), row(2));
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'quick' AND body PHRASE 'brown fox'"), row(1));
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'quick' AND body MATCH 'dog'"));
        // each phrase is matched on its own, the two phrases never join into one
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body PHRASE 'quick brown' AND body PHRASE 'fox brown'"));
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body PHRASE 'quick brown' AND body PHRASE 'brown fox'"), row(1));

        // the order of the relations, a duplicate relation and a third relation change nothing
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'fox' AND body MATCH 'quick'"), row(1), row(2));
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body PHRASE 'brown fox' AND body MATCH 'quick'"), row(1));
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'dog' AND body MATCH 'quick'"));
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'fox' AND body MATCH 'fox'"), row(1), row(2));
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'quick' AND body MATCH 'fox' AND body PHRASE 'brown fox'"), row(1));
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

    @Test
    public void severalAnalyzedRelationsBindEachMarker() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'Disk full on node 3')");
        execute("INSERT INTO %s (id, body) VALUES (2, 'timeout after 30s')");
        execute("INSERT INTO %s (id, body) VALUES (3, 'Connection timeout to db')");
        execute("INSERT INTO %s (id, body) VALUES (4, 'Timeout')");

        String phrase = "SELECT id FROM %s WHERE body MATCH ? AND body PHRASE ?";
        String range = "SELECT id FROM %s WHERE body MATCH ? AND body > ? AND body < ? ALLOW FILTERING";
        String in = "SELECT id FROM %s WHERE body MATCH ? AND body IN ? ALLOW FILTERING";

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute(phrase, "timeout", "after 30s"), row(2));
            assertRowsIgnoringOrder(execute(range, "timeout", "T", "U"), row(4));
            assertRowsIgnoringOrder(execute(in, "timeout", List.of("Timeout", "Disk full")), row(4));
            // IN ? bound to one value stays an IN, it is not parsed as =
            assertRowsIgnoringOrder(execute(in, "timeout", List.of("Timeout")), row(4));
        });

        Session session = sessionNet();
        assertRowsNet(session.execute(session.prepare(formatQuery(phrase)).bind("timeout", "after 30s")), row(2));
        assertRowsNet(session.execute(session.prepare(formatQuery(range)).bind("timeout", "T", "U")), row(4));
        assertRowsNet(session.execute(session.prepare(formatQuery(in)).bind("timeout", List.of("Timeout", "Disk full"))), row(4));
        assertRowsNet(session.execute(session.prepare(formatQuery(in)).bind("timeout", List.of("Timeout"))), row(4));

        // A function in the stock relation is one of the statement's functions
        String function = "SELECT id FROM %s WHERE body MATCH 'timeout' AND body > blobAsText(0x6d) ALLOW FILTERING";
        SelectStatement select = (SelectStatement) QueryProcessor.getStatement(formatQuery(function), ClientState.forInternalCalls());
        Set<String> functions = new HashSet<>();
        for (Function f : select.getFunctions())
            functions.add(f.name().name);
        assertTrue(functions.toString(), functions.contains("blobastext"));
        assertRowsIgnoringOrder(execute(function), row(2));
    }

    @Test
    public void analyzedOperatorsAreMultiExpression()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        TableMetadata table = currentTableMetadata();
        ColumnMetadata body = table.getColumn(ColumnIdentifier.getInterned("body", false));
        IndexTermType termType = IndexTermType.create(body, table.partitionKeyColumns(), IndexTarget.Type.SIMPLE);
        ByteBuffer value = UTF8Type.instance.decompose("quick");

        // each analyzed relation keeps its own index expression
        for (Operator operator : new Operator[]{ Operator.ANALYZER_MATCHES, Operator.PHRASE, Operator.ANALYZER_MATCHES_KEY, Operator.PHRASE_KEY })
            assertTrue(operator.toString(), termType.isMultiExpression(RowFilter.create(false).add(body, operator, value)));

        // the bounds of a range combine into one index expression
        for (Operator operator : new Operator[]{ Operator.EQ, Operator.GT, Operator.LT })
            assertFalse(operator.toString(), termType.isMultiExpression(RowFilter.create(false).add(body, operator, value)));
    }

    @Test
    public void wordSearchThenStockFilterOnOneColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'Disk full on node 3')");
        execute("INSERT INTO %s (id, body) VALUES (2, 'timeout after 30s')");
        execute("INSERT INTO %s (id, body) VALUES (3, 'Connection timeout to db')");
        execute("INSERT INTO %s (id, body) VALUES (4, 'Timeout')");

        // The index cannot answer a range or IN, so the query needs ALLOW FILTERING
        for (String query : new String[]{ "SELECT id FROM %s WHERE body MATCH 'timeout' AND body > 'm'",
                                          "SELECT id FROM %s WHERE body > 'm' AND body MATCH 'timeout'",
                                          "SELECT id FROM %s WHERE body MATCH 'timeout' AND body >= 'T' AND body < 'U'",
                                          "SELECT id FROM %s WHERE body >= 'T' AND body MATCH 'timeout' AND body < 'U'",
                                          "SELECT id FROM %s WHERE body MATCH 'timeout' AND body IN ('Timeout', 'Disk full')",
                                          "SELECT id FROM %s WHERE body IN ('Timeout', 'Disk full') AND body MATCH 'timeout'" })
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, query);

        beforeAndAfterFlush(this::assertWordSearchThenStockFilter);

        compact();
        waitForCompactionsFinished();
        assertWordSearchThenStockFilter();

        // A second stock relation merges with the first one as it does without the word search
        assertInvalidMessage("Column \"body\" cannot be restricted by both an equality and an inequality relation",
                             "SELECT id FROM %s WHERE body MATCH 'x' AND body > 'a' AND body = 'b' ALLOW FILTERING");
        assertInvalidMessage("body cannot be restricted by more than one relation if it includes a IN",
                             "SELECT id FROM %s WHERE body IN ('a', 'b') AND body MATCH 'x' AND body > 'a' ALLOW FILTERING");
        assertInvalidMessage("body cannot be restricted by more than one relation if it includes a IN",
                             "SELECT id FROM %s WHERE body MATCH 'x' AND body IN ('a', 'b') AND body > 'a' ALLOW FILTERING");
    }

    private void assertWordSearchThenStockFilter() throws Throwable
    {
        // The index finds the rows with the word, then the range or IN is checked on the whole raw value
        // in byte order, where 'Timeout' < 'm' < 'timeout after 30s'
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'timeout' AND body > 'm' ALLOW FILTERING"), row(2));
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body > 'm' AND body MATCH 'timeout' ALLOW FILTERING"), row(2));
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'timeout' AND body >= 'T' AND body < 'U' ALLOW FILTERING"), row(4));
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body >= 'T' AND body MATCH 'timeout' AND body < 'U' ALLOW FILTERING"), row(4));
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'timeout' AND body IN ('Timeout', 'Disk full') ALLOW FILTERING"), row(4));
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body IN ('Timeout', 'Disk full') AND body MATCH 'timeout' ALLOW FILTERING"), row(4));
    }

    @Test
    public void rangeOrInAloneKeepsStockFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'Disk full on node 3')");
        execute("INSERT INTO %s (id, body) VALUES (2, 'timeout after 30s')");
        execute("INSERT INTO %s (id, body) VALUES (3, 'Connection timeout to db')");
        execute("INSERT INTO %s (id, body) VALUES (4, 'Timeout')");

        String range = "SELECT id FROM %s WHERE body > 'm'";
        String in = "SELECT id FROM %s WHERE body IN ('Timeout', 'Disk full')";
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, range);
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, in);

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute(range + " ALLOW FILTERING"), row(2));
            assertRowsIgnoringOrder(execute(in + " ALLOW FILTERING"), row(4));
        });

        // Neither query uses the analyzed index
        for (String query : new String[]{ range, in })
        {
            SelectStatement select = (SelectStatement) QueryProcessor.getStatement(formatQuery(query + " ALLOW FILTERING"), ClientState.forInternalCalls());
            ReadCommand command = (ReadCommand) select.getQuery(QueryOptions.DEFAULT, FBUtilities.nowInSeconds());
            assertNull(query, command.indexQueryPlan());
        }
    }

    @Test
    public void rangesAndInOnOtherColumnsWithWordSearch() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text, n int, ts timestamp)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        createIndex("CREATE INDEX ON %s(n) USING 'sai'");

        execute("INSERT INTO %s (id, body, n, ts) VALUES (1, 'the quick brown fox', 7, '2026-02-01')");
        execute("INSERT INTO %s (id, body, n, ts) VALUES (2, 'quick fox brown', 3, '2025-06-01')");
        execute("INSERT INTO %s (id, body, n, ts) VALUES (3, 'lazy dog', 9, '2026-03-01')");
        execute("INSERT INTO %s (id, body, n, ts) VALUES (4, 'quick brown fox jumps', 1, '2026-05-01')");

        String in = "SELECT id FROM %s WHERE body MATCH 'quick' AND n IN (1, 7)";
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, in);

        beforeAndAfterFlush(() -> {
            // every relation has an index, so no ALLOW FILTERING
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'quick' AND n > 5"), row(1));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'quick' AND body PHRASE 'brown fox' AND n > 5"), row(1));
            // IN and a column without an index are checked by filtering
            assertRowsIgnoringOrder(execute(in + " ALLOW FILTERING"), row(1), row(4));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'quick' AND ts > '2026-01-01' ALLOW FILTERING"), row(1), row(4));
        });
    }

    @Test
    public void inIsRecheckedUnderReplicaFilteringProtectionWithoutDisjunction() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'Disk full on node 3')");
        execute("INSERT INTO %s (id, body) VALUES (2, 'timeout after 30s')");
        execute("INSERT INTO %s (id, body) VALUES (3, 'Connection timeout to db')");
        execute("INSERT INTO %s (id, body) VALUES (4, 'Timeout')");

        String withIn = "SELECT id FROM %s WHERE body MATCH 'timeout' AND body IN ('Timeout', 'Disk full') ALLOW FILTERING";
        String withSlice = "SELECT id FROM %s WHERE body MATCH 'timeout' AND body > 'm' ALLOW FILTERING";
        for (String query : new String[]{ withSlice, withIn })
        {
            SelectStatement select = (SelectStatement) QueryProcessor.getStatement(formatQuery(query), ClientState.forInternalCalls());
            ReadCommand command = (ReadCommand) select.getQuery(QueryOptions.DEFAULT, FBUtilities.nowInSeconds());

            if (query.equals(withIn))
            {
                // The post index filter holds exactly the IN, which the index filter leaves out
                List<RowFilter.Expression> post = command.indexQueryPlan().postIndexQueryFilter().getExpressions();
                assertEquals(post.toString(), 1, post.size());
                assertEquals(Operator.IN, post.get(0).operator());
                assertEquals("body", post.get(0).column().name.toString());
            }

            // Every row of the table, as the merged rows reach the coordinator re-check
            PartitionRangeReadCommand allData = PartitionRangeReadCommand.allDataRead(getCurrentColumnFamilyStore().metadata(), FBUtilities.nowInSeconds());
            Set<Integer> kept = new HashSet<>();
            try (ReadExecutionController controller = allData.executionController();
                 PartitionIterator partitions = command.indexSearcher().filterReplicaFilteringProtection(allData.executeInternal(controller)))
            {
                while (partitions.hasNext())
                {
                    try (RowIterator partition = partitions.next())
                    {
                        if (partition.hasNext())
                            kept.add(Int32Type.instance.compose(partition.partitionKey().getKey()));
                    }
                }
            }
            assertEquals(query, query.equals(withIn) ? ImmutableSet.of(4) : ImmutableSet.of(2), kept);
        }
    }

    @Test
    public void tracingReportsEachAnalyzedRelation() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'the quick brown fox')");
        flush();

        Session session = sessionNet();
        String query = "SELECT id FROM %s WHERE body MATCH 'quick' AND body PHRASE 'brown fox'";
        String matchTrace = getSingleTraceStatement(session, query, "Query analyzed body MATCH");
        assertNotNull(matchTrace);
        assertTrue(matchTrace, matchTrace.contains("quick@0"));
        String phraseTrace = getSingleTraceStatement(session, query, "Query analyzed body PHRASE");
        assertNotNull(phraseTrace);
        assertTrue(phraseTrace, phraseTrace.contains("brown@0"));
        assertTrue(phraseTrace, phraseTrace.contains("fox@1"));
    }

    @Test
    public void containsServedByALegacyIndexNeedsAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, tags set<text>)");
        createIndex("CREATE INDEX ON %s(tags) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        createIndex("CREATE INDEX ON %s(tags) USING 'legacy_local_table'");

        execute("INSERT INTO %s (id, tags) VALUES (1, {'red', 'quick fox'})");
        execute("INSERT INTO %s (id, tags) VALUES (2, {'blue', 'quick dog'})");
        execute("INSERT INTO %s (id, tags) VALUES (3, {'red', 'lazy dog'})");

        // Only the legacy index answers CONTAINS and only the analyzed index answers MATCH, so no one
        // index serves the query and it needs ALLOW FILTERING
        String containsFirst = "SELECT id FROM %s WHERE tags CONTAINS 'red' AND tags MATCH 'quick'";
        String matchFirst = "SELECT id FROM %s WHERE tags MATCH 'quick' AND tags CONTAINS 'red'";
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, containsFirst);
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, matchFirst);

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute(containsFirst + " ALLOW FILTERING"), row(1));
            assertRowsIgnoringOrder(execute(matchFirst + " ALLOW FILTERING"), row(1));
        });
    }
}
