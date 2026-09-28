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

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.index.sai.SAITester;
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
            // the : operator is the supported spelling
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE body : 'quick fox'"), row(1));
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
                       warnings.stream().anyMatch(w -> w.contains("= behaves like the : operator")));
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

        assertInvalidMessage("The : operator is not supported in conditions",
                             "UPDATE %s SET body = 'a' WHERE id = 1 IF body : 'quick'");
        assertInvalidMessage("The : operator is not supported in conditions",
                             "DELETE FROM %s WHERE id = 1 IF body : 'quick'");
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
}
