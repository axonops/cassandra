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
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * The behaviour matrix of {@code =} on analyzed columns, driven by the
 * {@code equals_behaviour_when_analyzed} index option, and the raw byte semantics of LWT
 * conditions next to it.
 */
public class EqualsBehaviourWhenAnalyzedQueryTest extends SAITester
{
    @Before
    public void setup()
    {
        requireNetwork();
    }

    @Test
    public void equalsIsRejectedByDefault() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'quick fox')");

        beforeAndAfterFlush(() -> {
            assertInvalidMessage(String.format(SingleColumnRestriction.EQRestriction.EQ_UNSUPPORTED_ON_ANALYZED_MESSAGE, "body"),
                                 "SELECT id FROM %s WHERE body = 'quick fox'");
            // the MATCH operator is the supported spelling
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body MATCH 'quick fox'"), row(1));
        });
    }

    @Test
    public void matchBehaviourRewritesEqualsAndWarns() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = " +
                    "{ 'index_analyzer' : 'standard', 'equals_behaviour_when_analyzed' : 'MATCH' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'the quick fox')");
        execute("INSERT INTO %s (id, body) VALUES (2, 'lazy dog')");

        beforeAndAfterFlush(() -> {
            // = behaves exactly like the analyzed match operator, token AND semantics included
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body = 'quick fox'"), row(1));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body = 'fox quick'"), row(1));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body = 'quick dog'"));

            ClientWarn.instance.captureWarnings();
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body = 'quick'"), row(1));
            List<String> warnings = ClientWarn.instance.getWarnings();
            assertNotNull(warnings);
            assertTrue(warnings.toString(),
                       warnings.stream().anyMatch(w -> w.contains("= behaves like the MATCH operator")));
            ClientWarn.instance.resetWarnings();
        });
    }

    @Test
    public void lwtConditionsCompareRawBytes() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = " +
                    "{ 'index_analyzer' : 'standard', 'equals_behaviour_when_analyzed' : 'MATCH' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'Quick Fox')");

        // IF = always compares the raw stored bytes, analysis never applies to conditions
        assertRows(execute("UPDATE %s SET body = 'a' WHERE id = 1 IF body = 'quick fox'"), row(false, "Quick Fox"));
        assertEquals("Quick Fox", execute("SELECT body FROM %s WHERE id = 1").one().getString("body"));

        assertRows(execute("UPDATE %s SET body = 'lazy dog' WHERE id = 1 IF body = 'Quick Fox'"), row(true));
        assertEquals("lazy dog", execute("SELECT body FROM %s WHERE id = 1").one().getString("body"));
    }

    @Test
    public void analyzedOperatorsAreRejectedInConditions() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'quick fox')");

        assertInvalidSyntax("UPDATE %s SET body = 'a' WHERE id = 1 IF body MATCH 'quick'");
        assertInvalidSyntax("DELETE FROM %s WHERE id = 1 IF body MATCH 'quick'");
    }

    @Test
    public void equalsStaysRawOnUnanalyzedIndexes() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai'");

        execute("INSERT INTO %s (id, body) VALUES (1, 'quick fox')");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body = 'quick fox'"), row(1));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body = 'quick'"));
        });
    }

    @Test
    public void equalsJoinsAnalyzedRelations() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = " +
                    "{ 'index_analyzer' : 'standard', 'equals_behaviour_when_analyzed' : 'MATCH' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'the quick fox')");
        execute("INSERT INTO %s (id, body) VALUES (2, 'lazy dog')");
        execute("INSERT INTO %s (id, body) VALUES (3, 'quick dog')");
        execute("INSERT INTO %s (id, body) VALUES (4, 'lazy fox')");

        beforeAndAfterFlush(() -> {
            // = is one more MATCH relation in either order, and the client gets one warning
            for (String query : new String[]{ "SELECT id FROM %s WHERE body = 'fox' AND body MATCH 'quick'",
                                              "SELECT id FROM %s WHERE body MATCH 'quick' AND body = 'fox'" })
            {
                ClientWarn.instance.captureWarnings();
                assertRowsIgnoringOrder(execute(query), row(1));
                List<String> warnings = ClientWarn.instance.getWarnings();
                assertNotNull(query, warnings);
                assertEquals(warnings.toString(), 1, warnings.stream().filter(w -> w.contains("= behaves like the MATCH operator")).count());
                ClientWarn.instance.resetWarnings();
            }
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body = ? AND body MATCH ?", "fox", "quick"), row(1));
        });

        // A function in the = term is one of the statement's functions
        String function = "SELECT id FROM %s WHERE body = blobAsText(0x666f78) AND body MATCH 'quick'";
        SelectStatement select = (SelectStatement) QueryProcessor.getStatement(formatQuery(function), ClientState.forInternalCalls());
        Set<String> functions = new HashSet<>();
        for (Function f : select.getFunctions())
            functions.add(f.name().name);
        assertTrue(functions.toString(), functions.contains("blobastext"));
        assertRowsIgnoringOrder(execute(function), row(1));

        // A second stock relation merges with the = as it does without the word search
        String equalText = "body cannot be restricted by more than one relation if it includes an Equal";
        assertInvalidMessage(equalText, "SELECT id FROM %s WHERE body = 'a' AND body MATCH 'x' AND body = 'b'");
        assertInvalidMessage(equalText, "SELECT id FROM %s WHERE body = 'a' AND body MATCH 'x' AND body > 'a' ALLOW FILTERING");
        assertInvalidMessage(equalText, "SELECT id FROM %s WHERE body MATCH 'x' AND body = 'a' AND body > 'a' ALLOW FILTERING");

        // Under UNSUPPORTED, = next to MATCH is refused as = alone is
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        execute("INSERT INTO %s (id, body) VALUES (1, 'the quick fox')");

        String unsupported = String.format(SingleColumnRestriction.EQRestriction.EQ_UNSUPPORTED_ON_ANALYZED_MESSAGE, "body");
        assertInvalidMessage(unsupported, "SELECT id FROM %s WHERE body = 'fox' AND body MATCH 'quick'");
        assertInvalidMessage(unsupported, "SELECT id FROM %s WHERE body MATCH 'quick' AND body = 'fox'");
        // an IN list written with one value is parsed as =
        assertInvalidMessage(unsupported, "SELECT id FROM %s WHERE body IN ('fox') AND body MATCH 'quick'");
        assertInvalidMessage(unsupported, "SELECT id FROM %s WHERE body MATCH 'quick' AND body IN ('fox')");
    }
}
