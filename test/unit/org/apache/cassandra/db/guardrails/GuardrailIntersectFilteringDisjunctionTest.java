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

package org.apache.cassandra.db.guardrails;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.junit.Assert.fail;

/**
 * Tests the intersect filtering guardrail on a query with OR whose root condition the SAI indexes do not
 * serve. Such a query filters, as it does without OR, so above ONE it meets the guardrail. The keyspace has
 * RF 3 on this single node, so QUORUM needs reconciliation.
 *
 * @see Guardrails#intersectFilteringQueryEnabled
 */
public class GuardrailIntersectFilteringDisjunctionTest extends GuardrailTester
{
    private static final String WARNING = "Filtering query with intersection on mutable columns at consistency level " +
                                          "requiring coordinator reconciliation is not recommended";
    private static final String FAILURE = "Filtering query with intersection on mutable columns at consistency level " +
                                          "requiring coordinator reconciliation is not allowed";

    private boolean previousWarned;
    private boolean previousEnabled;
    private String table;

    @Before
    public void setupTest()
    {
        previousWarned = guardrails().getIntersectFilteringQueryWarned();
        previousEnabled = guardrails().getIntersectFilteringQueryEnabled();
        guardrails().setIntersectFilteringQueryWarned(true);
        guardrails().setIntersectFilteringQueryEnabled(true);

        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
        table = keyspace + '.' + createTable(keyspace, "CREATE TABLE %s (pk int PRIMARY KEY, s text, a int, b int)");
        createIndex(keyspace, "CREATE CUSTOM INDEX ON %s(s) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = { 'mode' : 'PREFIX' }");
        createIndex(keyspace, "CREATE INDEX ON %s(a) USING 'sai'");
        createIndex(keyspace, "CREATE INDEX ON %s(b) USING 'sai'");

        execute("INSERT INTO " + table + " (pk, s, a, b) VALUES (1, 'abcdef', 1, 9)");
        execute("INSERT INTO " + table + " (pk, s, a, b) VALUES (2, 'zzz', 1, 9)");
        execute("INSERT INTO " + table + " (pk, s, a, b) VALUES (3, 'abcxyz', 9, 2)");
        execute("INSERT INTO " + table + " (pk, s, a, b) VALUES (4, 'qqq', 9, 2)");
    }

    @After
    public void teardownTest()
    {
        guardrails().setIntersectFilteringQueryWarned(previousWarned);
        guardrails().setIntersectFilteringQueryEnabled(previousEnabled);
    }

    @Test
    public void testDisjunctionNextToUnservedConditionWarnsAboveOne() throws Throwable
    {
        String query = "SELECT pk FROM " + table + " WHERE s LIKE 'abc%%' AND (a = 1 OR b = 2) ALLOW FILTERING";

        // The guardrail warns when the statement builds its row filter. One node cannot then answer QUORUM for RF 3.
        assertWarns(() -> assertUnavailableAtQuorum(query), WARNING);

        // At ONE the read needs no reconciliation and returns the rows of the query
        assertValid(() -> assertRowsIgnoringOrder(rowsAtOne(query), row(1), row(3)));
    }

    @Test
    public void testDisjunctionNextToUnservedConditionIsRefusedWhenDisabled() throws Throwable
    {
        guardrails().setIntersectFilteringQueryEnabled(false);

        assertFails(() -> execute(userClientState, "SELECT pk FROM " + table + " WHERE s LIKE 'abc%%' AND (a = 1 OR b = 2) ALLOW FILTERING", ConsistencyLevel.QUORUM),
                    FAILURE);

        // Control. At ONE the guardrail does not apply
        assertValid(() -> assertRowsIgnoringOrder(rowsAtOne("SELECT pk FROM " + table + " WHERE s LIKE 'abc%%' AND (a = 1 OR b = 2) ALLOW FILTERING"), row(1), row(3)));
    }

    @Test
    public void testConditionWithoutDisjunctionWarnsAboveOne() throws Throwable
    {
        // Control. The same conditions without OR meet the guardrail in Apache Cassandra
        String query = "SELECT pk FROM " + table + " WHERE s LIKE 'abc%%' AND a = 1 ALLOW FILTERING";
        assertWarns(() -> assertUnavailableAtQuorum(query), WARNING);
        assertValid(() -> assertRowsIgnoringOrder(rowsAtOne(query), row(1)));
    }

    @Test
    public void testServedDisjunctionDoesNotWarn() throws Throwable
    {
        // Control. A root condition the SAI indexes serve keeps the query off the filtering rule
        assertValid(() -> assertUnavailableAtQuorum("SELECT pk FROM " + table + " WHERE a = 1 AND (a = 2 OR b = 2)"));
    }

    private void assertUnavailableAtQuorum(String query)
    {
        try
        {
            execute(userClientState, query, ConsistencyLevel.QUORUM);
            fail("One node should not answer QUORUM for RF 3: " + query);
        }
        catch (UnavailableException e)
        {
            // expected once the row filter is built
        }
    }

    private UntypedResultSet rowsAtOne(String query)
    {
        ResultMessage.Rows rows = (ResultMessage.Rows) execute(userClientState, query, ConsistencyLevel.ONE);
        return UntypedResultSet.create(rows.result);
    }
}
