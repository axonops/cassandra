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

package org.apache.cassandra.distributed.test.sai;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.ClusterVersionGate;
import org.apache.cassandra.service.ClientState;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * OR queries on a 2 node cluster with node 1 in datacenter1 and node 2 in datacenter2. The test keyspace
 * holds one replica in each data center, so LOCAL_QUORUM and LOCAL_SERIAL through node 1 read node 1 alone,
 * while QUORUM, EACH_QUORUM and SERIAL read both nodes. A second keyspace holds two replicas in datacenter1,
 * where LOCAL_QUORUM blocks for two.
 */
public class DisjunctionMultiDcDistributedTest extends TestBaseImpl
{
    private static final int NODES = 2;

    private static final String TWO_LOCAL_REPLICAS = "two_local_replicas";

    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws Exception
    {
        // OR queries are refused unless every node advertises the fork messaging version, which
        // nodes in a before-5 storage compatibility mode do not
        CLUSTER = Cluster.build(NODES)
                         .withDCs(2)
                         .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                     .set("storage_compatibility_mode", "NONE")
                                                     .with(GOSSIP).with(NETWORK))
                         .start();
        CLUSTER.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1, 'datacenter2': 1}"));
        // datacenter1 has one node, which is enough to prepare statements against this keyspace
        CLUSTER.schemaChange("CREATE KEYSPACE " + TWO_LOCAL_REPLICAS + " WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 2, 'datacenter2': 1}");
        waitForForkVersionAgreement();
    }

    @AfterClass
    public static void shutDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void disjunctionsReadFromOneLocalReplica()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.local_split (pk int, ck int, a text, b text, c text, d text, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'"));
        for (String column : new String[]{ "a", "b", "c", "d" })
            CLUSTER.schemaChange(withKeyspace("CREATE INDEX local_split_" + column + "_idx ON %s.local_split(" + column + ") USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // One partition, so clustering order fixes what each page holds. Decoys sort first and match
        // only the first disjunction. ck 20 holds its newest c = '1' on node 1 and its newest a = '1'
        // on node 2, so it matches only on the merged row.
        for (int ck = 0; ck <= 5; ck++)
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.local_split (pk, ck, a, b, c, d) VALUES (0, ?, '1', '0', '0', '0')"),
                                           ConsistencyLevel.ALL, ck);
        for (int ck = 10; ck <= 12; ck++)
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.local_split (pk, ck, a, b, c, d) VALUES (0, ?, '1', '0', '1', '0')"),
                                           ConsistencyLevel.ALL, ck);
        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.local_split (pk, ck, a, b, c, d) VALUES (0, 20, '0', '0', '0', '0') USING TIMESTAMP 1"),
                                       ConsistencyLevel.ALL);
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.local_split USING TIMESTAMP 2 SET c = '1' WHERE pk = 0 AND ck = 20"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.local_split USING TIMESTAMP 2 SET a = '1' WHERE pk = 0 AND ck = 20"));

        String query = "SELECT ck FROM %s.local_split WHERE (a = '1' OR b = '2') AND (c = '1' OR d = '2')";

        // Control. A read that resolves both replicas keeps the row split across them
        for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.QUORUM, ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.SERIAL })
        {
            assertEquals(cl.name(), ImmutableSet.of(10, 11, 12, 20), valuesAt(query, cl));
            for (int pageSize : new int[]{ 1, 2, 100 })
                assertEquals(cl + " page size " + pageSize, ImmutableSet.of(10, 11, 12, 20), pagedValuesAt(query, cl, pageSize));
        }

        // Control. ck 20 is not a match on one replica alone
        for (int pageSize : new int[]{ 1, 2, 100 })
            assertEquals("ONE page size " + pageSize, ImmutableSet.of(10, 11, 12), pagedValuesAt(query, ConsistencyLevel.ONE, pageSize));

        // Control. Plain AND at LOCAL_QUORUM keeps the Apache behaviour. Node 1 keeps every row matching
        // a = '1' or c = '1' under the strictness downgrade for unrepaired matches, the coordinator re-check
        // drops the decoys that fill the page, and with one response there is no short read protection, so
        // paging stops. The fork leaves it unchanged, and the assertion makes any change a visible decision.
        String stock = "SELECT ck FROM %s.local_split WHERE a = '1' AND c = '1'";
        assertTrue(needsReconciliationAt(stock, ConsistencyLevel.LOCAL_QUORUM));
        assertEquals(ImmutableSet.of(), pagedValuesAt(stock, ConsistencyLevel.LOCAL_QUORUM, 2));
        assertEquals(0, CLUSTER.coordinator(1).execute(withKeyspace(stock + " LIMIT 2"), ConsistencyLevel.LOCAL_QUORUM).length);

        // Control. IN next to OR stays refused on a read that resolves both replicas
        String in = "SELECT ck FROM %s.local_split WHERE b IN ('0', '5') AND (a = '1' OR d = '2') ALLOW FILTERING";
        try
        {
            CLUSTER.coordinator(1).execute(withKeyspace(in), ConsistencyLevel.QUORUM);
            fail("IN next to OR should be refused at QUORUM");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("is only supported in intersections for reads that do not require replica reconciliation"));
        }

        // Control. The same IN without OR is refused on the one local replica, because the filter is not strict
        // there: it intersects two regular columns under reconciliation, as in Apache Cassandra. The assertion
        // makes any change a visible decision.
        String stockIn = "SELECT ck FROM %s.local_split WHERE b IN ('0', '5') AND a = '1' ALLOW FILTERING";
        for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_SERIAL })
        {
            try
            {
                CLUSTER.coordinator(1).execute(withKeyspace(stockIn), cl);
                fail("IN without OR should be refused at " + cl);
            }
            catch (RuntimeException e)
            {
                assertTrue(e.getMessage(), e.getMessage().contains("is only supported in intersections for reads that do not require replica reconciliation"));
            }
        }

        // The one local replica filters strictly, as at ONE, so decoys never fill a page
        for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_SERIAL })
        {
            assertFalse(cl.name(), needsReconciliationAt(query, cl));
            for (int pageSize : new int[]{ 1, 2, 100 })
                assertEquals(cl + " page size " + pageSize, ImmutableSet.of(10, 11, 12), pagedValuesAt(query, cl, pageSize));
        }

        assertRows(CLUSTER.coordinator(1).execute(withKeyspace(query + " LIMIT 2"), ConsistencyLevel.LOCAL_QUORUM),
                   row(10), row(11));

        // A condition next to OR that is not IN is accepted there too, and decoys matching only a = '1' never fill a page
        String equality = "SELECT ck FROM %s.local_split WHERE a = '1' AND (c = '1' OR d = '2')";
        for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_SERIAL })
        {
            for (int pageSize : new int[]{ 1, 2, 100 })
                assertEquals(cl + " page size " + pageSize, ImmutableSet.of(10, 11, 12), pagedValuesAt(equality, cl, pageSize));
        }

        // IN next to OR is refused on the one local replica as the same IN without OR is, since whether a query
        // is accepted depends on the consistency level and the replication factor, not on how many replicas it reads
        for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_SERIAL })
        {
            try
            {
                CLUSTER.coordinator(1).execute(withKeyspace(in), cl);
                fail("IN next to OR should be refused at " + cl);
            }
            catch (RuntimeException e)
            {
                assertTrue(e.getMessage(), e.getMessage().contains("is only supported in intersections for reads that do not require replica reconciliation"));
            }
        }
    }

    @Test
    public void disjunctionsReadFromOneLocalReplicaByFiltering()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.local_filter (pk int, ck int, a text, b text, x text, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'"));

        // One partition. Decoys sort first and match x = '1', the condition a replica checks for the
        // AND group, and neither disjunct. ck 20 holds its newest x = '1' on node 1 and its newest
        // a = '1' on node 2, so it matches only on the merged row.
        for (int ck = 0; ck <= 5; ck++)
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.local_filter (pk, ck, a, b, x) VALUES (0, ?, '0', '0', '1')"),
                                           ConsistencyLevel.ALL, ck);
        for (int ck = 10; ck <= 12; ck++)
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.local_filter (pk, ck, a, b, x) VALUES (0, ?, '1', '0', '1')"),
                                           ConsistencyLevel.ALL, ck);
        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.local_filter (pk, ck, a, b, x) VALUES (0, 20, '0', '0', '0') USING TIMESTAMP 1"),
                                       ConsistencyLevel.ALL);
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.local_filter USING TIMESTAMP 2 SET x = '1' WHERE pk = 0 AND ck = 20"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.local_filter USING TIMESTAMP 2 SET a = '1' WHERE pk = 0 AND ck = 20"));

        String query = "SELECT ck FROM %s.local_filter WHERE x = '1' AND (a = '1' OR b = '2') ALLOW FILTERING";

        // Control. A read that resolves both replicas keeps the row split across them
        assertTrue(needsReconciliationAt(query, ConsistencyLevel.QUORUM));
        for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.QUORUM, ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.SERIAL })
        {
            assertEquals(cl.name(), ImmutableSet.of(10, 11, 12, 20), valuesAt(query, cl));
            for (int pageSize : new int[]{ 1, 2, 100 })
                assertEquals(cl + " page size " + pageSize, ImmutableSet.of(10, 11, 12, 20), pagedValuesAt(query, cl, pageSize));
        }

        // Control. ck 20 is not a match on one replica alone
        for (int pageSize : new int[]{ 1, 2, 100 })
            assertEquals("ONE page size " + pageSize, ImmutableSet.of(10, 11, 12), pagedValuesAt(query, ConsistencyLevel.ONE, pageSize));

        // Control. Plain AND with ALLOW FILTERING at LOCAL_QUORUM keeps the Apache behaviour. The replica
        // checks the whole AND, and the filter still needs reconciliation. The fork leaves it unchanged,
        // and the assertion makes any change a visible decision.
        String stock = "SELECT ck FROM %s.local_filter WHERE x = '1' AND a = '1' ALLOW FILTERING";
        assertTrue(needsReconciliationAt(stock, ConsistencyLevel.LOCAL_QUORUM));
        for (int pageSize : new int[]{ 1, 2, 100 })
            assertEquals("plain AND page size " + pageSize, ImmutableSet.of(10, 11, 12), pagedValuesAt(stock, ConsistencyLevel.LOCAL_QUORUM, pageSize));

        // The one local replica filters strictly, as at ONE, so decoys never fill a page
        for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_SERIAL })
        {
            assertFalse(cl.name(), needsReconciliationAt(query, cl));
            for (int pageSize : new int[]{ 1, 2, 100 })
                assertEquals(cl + " page size " + pageSize, ImmutableSet.of(10, 11, 12), pagedValuesAt(query, cl, pageSize));
        }

        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT ck FROM %s.local_filter WHERE x = '1' AND (a = '1' OR b = '2') LIMIT 2 ALLOW FILTERING"),
                                                  ConsistencyLevel.LOCAL_QUORUM),
                   row(10), row(11));

        // With no index nothing refuses IN, so IN next to OR runs and the one local replica filters it strictly too
        String in = "SELECT ck FROM %s.local_filter WHERE x IN ('1', '5') AND (a = '1' OR b = '2') ALLOW FILTERING";
        for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_SERIAL })
        {
            for (int pageSize : new int[]{ 1, 2, 100 })
                assertEquals(cl + " IN page size " + pageSize, ImmutableSet.of(10, 11, 12), pagedValuesAt(in, cl, pageSize));
        }
    }

    @Test
    public void disjunctionsReadFromTwoLocalReplicasNeedReconciliation()
    {
        CLUSTER.schemaChange("CREATE TABLE " + TWO_LOCAL_REPLICAS + ".two_local (pk int, ck int, a text, b text, x text, PRIMARY KEY (pk, ck))");

        // LOCAL_QUORUM and LOCAL_SERIAL block for two replicas in datacenter1, so short read protection
        // runs and the replicas keep rows that match part of the filter
        String query = "SELECT ck FROM " + TWO_LOCAL_REPLICAS + ".two_local WHERE x = '1' AND (a = '1' OR b = '2') ALLOW FILTERING";
        assertFalse(needsReconciliationAt(query, ConsistencyLevel.ONE));
        assertTrue(needsReconciliationAt(query, ConsistencyLevel.LOCAL_QUORUM));
        assertTrue(needsReconciliationAt(query, ConsistencyLevel.LOCAL_SERIAL));
    }

    /**
     * @return the first selected column of every row, checking that no row is returned twice
     */
    private static Set<Object> valuesAt(String select, ConsistencyLevel cl)
    {
        Object[][] rows = CLUSTER.coordinator(1).execute(withKeyspace(select), cl);
        Set<Object> values = Arrays.stream(rows).map(row -> row[0]).collect(Collectors.toSet());
        assertEquals("duplicate rows in " + Arrays.deepToString(rows), rows.length, values.size());
        return values;
    }

    /**
     * @return the first selected column of every row read page by page, checking that no row is returned twice
     */
    private static Set<Object> pagedValuesAt(String select, ConsistencyLevel cl, int pageSize)
    {
        Set<Object> values = new HashSet<>();
        int count = 0;
        Iterator<Object[]> pages = CLUSTER.coordinator(1).executeWithPaging(withKeyspace(select), cl, pageSize);
        while (pages.hasNext())
        {
            values.add(pages.next()[0]);
            count++;
        }
        assertEquals("duplicate rows at page size " + pageSize, count, values.size());
        return values;
    }

    /**
     * @return whether the row filter node 1 builds for the select at this consistency level needs reconciliation
     */
    private static boolean needsReconciliationAt(String select, ConsistencyLevel cl)
    {
        String query = withKeyspace(select);
        String level = cl.name();
        return CLUSTER.get(1).callOnInstance(() -> {
            SelectStatement statement = (SelectStatement) QueryProcessor.getStatement(query, ClientState.forInternalCalls());
            QueryOptions options = QueryOptions.forInternalCalls(org.apache.cassandra.db.ConsistencyLevel.valueOf(level), Collections.emptyList());
            return statement.getRowFilter(options, ClientState.forInternalCalls()).needsReconciliation();
        });
    }

    /**
     * OR is gated on every live peer advertising the fork messaging version, which nodes learn
     * through the handshake and gossip. Waits until every node's gate opens so the tests never
     * race that propagation.
     */
    private static void waitForForkVersionAgreement()
    {
        for (int node = 1; node <= NODES; node++)
        {
            CLUSTER.get(node).runOnInstance(() -> {
                for (int attempt = 0; attempt < 600; attempt++)
                {
                    try
                    {
                        ClusterVersionGate.checkClusterSupports("The distributed test setup");
                        return;
                    }
                    catch (InvalidRequestException e)
                    {
                        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                    }
                }
                throw new AssertionError("The cluster never agreed on the fork messaging version");
            });
        }
    }
}
