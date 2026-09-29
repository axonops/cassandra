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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.ScheduledThreadPoolExecutorPlus;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.StorageProxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Plain AND with ALLOW FILTERING and no index on rows whose matching values sit on different replicas, at
 * consistency levels that need reconciliation.
 */
public class PlainAndFilteringDistributedTest extends TestBaseImpl
{
    private static final int NODES = 3;

    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws Exception
    {
        CLUSTER = init(Cluster.build(NODES)
                              .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                          .with(GOSSIP).with(NETWORK))
                              .start(),
                       NODES);
    }

    @AfterClass
    public static void shutDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void plainAndSplitAcrossReplicasByFiltering()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.af_split (pk int, ck int, a text, b text, x text, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'"));

        // The three split rows share one partition, so a QUORUM read contacts one replica pair.
        // Each row holds its newest x = '1' on one node and its newest a = '1' on the next node, so no
        // replica matches the query on its own: ck 10 splits across nodes 1 and 2, ck 11 across nodes 2
        // and 3, ck 12 across nodes 3 and 1. Whatever pair QUORUM contacts, exactly one row has both
        // halves inside it.
        for (int ck = 10; ck <= 12; ck++)
        {
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.af_split (pk, ck, a, b, x) VALUES (0, ?, '0', '0', '0') USING TIMESTAMP 1"),
                                           ConsistencyLevel.ALL, ck);
            CLUSTER.get(ck - 9).executeInternal(withKeyspace("UPDATE %s.af_split USING TIMESTAMP 2 SET x = '1' WHERE pk = 0 AND ck = ?"), ck);
            CLUSTER.get((ck - 9) % NODES + 1).executeInternal(withKeyspace("UPDATE %s.af_split USING TIMESTAMP 2 SET a = '1' WHERE pk = 0 AND ck = ?"), ck);
        }

        // Decoys sort first. Every replica keeps them on a = '1' and the coordinator re-check drops them,
        // so replica pages fill with rows the coordinator discards and short reads follow
        for (int ck = 0; ck <= 5; ck++)
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.af_split (pk, ck, a, b, x) VALUES (0, ?, '1', '0', '0')"),
                                           ConsistencyLevel.ALL, ck);

        // Control. One column is never split across replicas
        assertEquals(ImmutableSet.of(0, 1, 2, 3, 4, 5, 10, 11, 12), valuesAt("SELECT ck FROM %s.af_split WHERE a = '1' ALLOW FILTERING", ConsistencyLevel.ALL));

        // Control. A read at ONE sees one replica, where no row matches in full
        String query = "SELECT ck FROM %s.af_split WHERE x = '1' AND a = '1' ALLOW FILTERING";
        assertEquals(0, CLUSTER.coordinator(1).execute(withKeyspace(query), ConsistencyLevel.ONE).length);

        assertEquals(ImmutableSet.of(10, 11, 12), valuesAt(query, ConsistencyLevel.ALL));
        assertEquals(1, CLUSTER.coordinator(1).execute(withKeyspace(query), ConsistencyLevel.QUORUM).length);
        assertEquals(ImmutableSet.of(10, 11, 12), valuesAt("SELECT ck FROM %s.af_split WHERE x = '1' AND a = '1' AND b = '0' ALLOW FILTERING", ConsistencyLevel.ALL));
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT COUNT(*) FROM %s.af_split WHERE x = '1' AND a = '1' ALLOW FILTERING"), ConsistencyLevel.ALL),
                   row(3L));

        for (int pageSize : new int[]{ 1, 2, 100 })
            assertEquals("page size " + pageSize, ImmutableSet.of(10, 11, 12), pagedValuesAt(CLUSTER, query, ConsistencyLevel.ALL, pageSize));

        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT ck FROM %s.af_split WHERE x = '1' AND a = '1' LIMIT 2 ALLOW FILTERING"), ConsistencyLevel.ALL),
                   row(10), row(11));
    }

    @Test
    public void plainAndSplitInOnePartitionReadsEveryReplica()
    {
        // With no speculation QUORUM contacts exactly two replicas
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.af_partition (pk int, ck int, a text, x text, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE' AND speculative_retry = 'NONE'"));
        writeSplitRowsAndDecoys("af_partition");

        // Control. Every replica keeps the pk 1 decoys on a = '1', so a response returned without the
        // coordinator re-check would show them
        String decoys = "SELECT ck FROM %s.af_partition WHERE pk = 1 AND x = '1' AND a = '1' ALLOW FILTERING";
        for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL })
            assertEquals(cl.name(), 0, CLUSTER.coordinator(1).execute(withKeyspace(decoys), cl).length);

        String query = "SELECT ck FROM %s.af_partition WHERE pk = 0 AND x = '1' AND a = '1' ALLOW FILTERING";
        assertEquals(ImmutableSet.of(10, 11, 12), valuesAt(query, ConsistencyLevel.ALL));
        assertEquals(1, CLUSTER.coordinator(1).execute(withKeyspace(query), ConsistencyLevel.QUORUM).length);
        assertEquals(ImmutableSet.of(10, 11, 12), valuesAt("SELECT ck FROM %s.af_partition WHERE pk IN (0, 1) AND x = '1' AND a = '1' ALLOW FILTERING", ConsistencyLevel.ALL));
        for (int pageSize : new int[]{ 1, 2 })
            assertEquals("page size " + pageSize, ImmutableSet.of(10, 11, 12), pagedValuesAt(CLUSTER, query, ConsistencyLevel.ALL, pageSize));
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT ck FROM %s.af_partition WHERE pk = 0 AND x = '1' AND a = '1' LIMIT 1 ALLOW FILTERING"), ConsistencyLevel.ALL),
                   row(10));

        // With ALWAYS a QUORUM read contacts all three replicas, which are all the candidates, so the
        // coordinator does not speculate
        CLUSTER.schemaChange(withKeyspace("ALTER TABLE %s.af_partition WITH speculative_retry = 'ALWAYS'"));
        long speculated = speculativeRetries(CLUSTER, "af_partition");
        assertEquals(0, CLUSTER.coordinator(1).execute(withKeyspace(decoys), ConsistencyLevel.QUORUM).length);
        // The resolver merges whatever arrived by the time two replicas answered
        Set<Object> always = valuesAt(query, ConsistencyLevel.QUORUM);
        assertFalse(always.isEmpty());
        assertTrue(always.toString(), ImmutableSet.of(10, 11, 12).containsAll(always));
        // Control
        assertEquals(speculated, speculativeRetries(CLUSTER, "af_partition"));

        // Control. SERIAL reads keep the Apache behaviour. The coordinator does not ask replicas for partial
        // matches, because Paxos would not linearize the extra reads that follow.
        assertSerialReadsKeepApacheBehaviour(query, decoys);
        CLUSTER.forEach(instance -> instance.runOnInstance(() -> StorageProxy.instance.setPaxosVariant("v2")));
        try
        {
            assertSerialReadsKeepApacheBehaviour(query, decoys);
        }
        finally
        {
            CLUSTER.forEach(instance -> instance.runOnInstance(() -> StorageProxy.instance.setPaxosVariant("v1")));
        }
    }

    @Test
    public void plainAndStaticSplitByFiltering()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.af_static (pk int, ck int, s text static, t text static, a text, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'"));

        // Static cells belong to the partition, so each case is its own partition, read at ALL.
        // pk 0 matches only on the merged partition: node 1 holds s, node 2 holds the row
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.af_static SET s = '1' WHERE pk = 0"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.af_static SET a = '1' WHERE pk = 0 AND ck = 1"));
        // pk 1 matches s and not a on every replica
        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.af_static (pk, ck, s, a) VALUES (1, 1, '1', '0')"), ConsistencyLevel.ALL);
        // pk 2: node 1 holds the newest statics and no rows, nodes 2 and 3 hold the row under older statics
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.af_static USING TIMESTAMP 2 SET s = '1', t = '0' WHERE pk = 2"));
        for (int node = 2; node <= NODES; node++)
            CLUSTER.get(node).executeInternal(withKeyspace("INSERT INTO %s.af_static (pk, ck, s, t, a) VALUES (2, 1, '0', '0', '1') USING TIMESTAMP 1"));
        // pk 3: the newest s = '1' is on node 1 and the newest t = '1' on node 2
        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.af_static (pk, ck, s, t, a) VALUES (3, 1, '0', '0', '0') USING TIMESTAMP 1"), ConsistencyLevel.ALL);
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.af_static USING TIMESTAMP 2 SET s = '1' WHERE pk = 3"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.af_static USING TIMESTAMP 2 SET t = '1' WHERE pk = 3"));
        // pk 4: nodes 2 and 3 hold the row under an older s, node 1 holds only the newest s
        for (int node = 2; node <= NODES; node++)
            CLUSTER.get(node).executeInternal(withKeyspace("INSERT INTO %s.af_static (pk, ck, s, a) VALUES (4, 1, '0', '1') USING TIMESTAMP 1"));
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.af_static USING TIMESTAMP 2 SET s = '1' WHERE pk = 4"));

        // A static and a clustering column: node 2 holds the static, node 1 the row
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.af_static_ck (pk int, ck0 int, ck1 int, s0 int static, v0 int, PRIMARY KEY (pk, ck0, ck1)) WITH read_repair = 'NONE'"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.af_static_ck (pk, ck0, ck1, s0, v0) VALUES (0, 1, 2, 3, 4)"));
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.af_static_ck SET v0 = 5 WHERE pk = 0 AND ck0 = 6 AND ck1 = 7"));

        // Control. One static column is never split across replicas
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk, ck FROM %s.af_static WHERE s = '1' ALLOW FILTERING"), ConsistencyLevel.ALL),
                   row(1, 1), row(0, 1), row(2, 1), row(4, 1), row(3, 1));

        // The replicas check a. (0, 1), (2, 1) and (4, 1) come back through the row fetched from node 1,
        // which also returns node 1's static row.
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk, ck FROM %s.af_static WHERE s = '1' AND a = '1' ALLOW FILTERING"), ConsistencyLevel.ALL),
                   row(0, 1), row(2, 1), row(4, 1));
        // Only static columns are restricted, so the replicas check s and node 1 keeps every row of pk 3
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk, ck FROM %s.af_static WHERE s = '1' AND t = '1' ALLOW FILTERING"), ConsistencyLevel.ALL),
                   row(3, 1));
        // Partition reads
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk, ck FROM %s.af_static WHERE pk = 0 AND s = '1' AND a = '1' ALLOW FILTERING"), ConsistencyLevel.ALL),
                   row(0, 1));
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk, ck FROM %s.af_static WHERE pk = 4 AND s = '1' AND a = '1' ALLOW FILTERING"), ConsistencyLevel.ALL),
                   row(4, 1));
        // Node 2 returns the partition with its static row and no rows, so the coordinator reads the whole
        // partition from node 1
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT * FROM %s.af_static_ck WHERE s0 = 3 AND ck1 = 7 ALLOW FILTERING"), ConsistencyLevel.ALL),
                   row(0, 6, 7, 3, 5));
    }

    @Test
    public void plainAndSplitIsReadRepairedOnPartitionRead()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.af_repair (pk int, ck int, a text, x text, PRIMARY KEY (pk, ck))"));
        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.af_repair (pk, ck, a, x) VALUES (0, 1, '0', '0') USING TIMESTAMP 1"), ConsistencyLevel.ALL);
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.af_repair USING TIMESTAMP 2 SET x = '1' WHERE pk = 0 AND ck = 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.af_repair USING TIMESTAMP 2 SET a = '1' WHERE pk = 0 AND ck = 1"));

        // Control. No replica matches on its own
        String query = "SELECT ck FROM %s.af_repair WHERE pk = 0 AND x = '1' AND a = '1' ALLOW FILTERING";
        for (int node = 1; node <= NODES; node++)
            assertRows(CLUSTER.get(node).executeInternal(withKeyspace(query)));

        // The responses disagree, so the read repair round runs as on a digest mismatch and writes the merged
        // row to every replica
        long repaired = CLUSTER.get(1).callOnInstance(() -> ReadRepairMetrics.repairedBlocking.getCount());
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace(query), ConsistencyLevel.ALL), row(1));
        assertEquals(repaired + 1, (long) CLUSTER.get(1).callOnInstance(() -> ReadRepairMetrics.repairedBlocking.getCount()));
        for (int node = 1; node <= NODES; node++)
            assertRows(CLUSTER.get(node).executeInternal(withKeyspace(query)), row(1));
    }

    @Test
    public void plainAndPartitionReadSpeculatesWithData()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.af_spec (pk int, ck int, a text, x text, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE' AND speculative_retry = '2000ms'"));
        writeSplitRowsAndDecoys("af_spec");

        // Speculate after 50 ms. The updater would reset the sampled latency, so it is removed, as in
        // ReadSpeculationTest.
        CLUSTER.get(1).runOnInstance(() -> {
            ((ScheduledThreadPoolExecutorPlus) ScheduledExecutors.optionalTasks).remove(CassandraDaemon.SPECULATION_THRESHOLD_UPDATER);
            Keyspace.open(KEYSPACE).getColumnFamilyStore("af_spec").sampleReadLatencyMicros = TimeUnit.MILLISECONDS.toMicros(50);
        });

        // Node 2 hangs from node 1's view but is not marked down. Rely on IP order, as ReadSpeculationTest
        // does: QUORUM through node 1 contacts nodes 1 and 2, then speculates on node 3.
        CLUSTER.filters().allVerbs().from(1).to(2).drop();
        try
        {
            long speculated = speculativeRetries(CLUSTER, "af_spec");

            // Control. The speculative response is data, so the decoys it holds reach the coordinator re-check
            assertEquals(0, CLUSTER.coordinator(1).execute(withKeyspace("SELECT ck FROM %s.af_spec WHERE pk = 1 AND x = '1' AND a = '1' ALLOW FILTERING"),
                                                           ConsistencyLevel.QUORUM).length);

            // ck 12 splits across nodes 3 and 1
            assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT ck FROM %s.af_spec WHERE pk = 0 AND x = '1' AND a = '1' ALLOW FILTERING"),
                                                      ConsistencyLevel.QUORUM),
                       row(12));
            assertEquals(speculated + 2, speculativeRetries(CLUSTER, "af_spec"));
        }
        finally
        {
            CLUSTER.filters().reset();
        }
    }

    @Test
    public void plainAndLocalQuorumWithOneLocalReplicaStaysStrict() throws Exception
    {
        // Node 1 in datacenter1, node 2 in datacenter2, one replica in each. Its own subnet keeps it clear of
        // the shared cluster, as in PagingTest.
        try (Cluster cluster = Cluster.build(2)
                                      .withRacks(2, 1, 1)
                                      .withSubnet(1)
                                      .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                                  .with(GOSSIP).with(NETWORK))
                                      .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1, 'datacenter2': 1}"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.af_dc (pk int, ck int, a text, x text, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'"));

            // (0, 10) holds its newest x = '1' on node 1 and its newest a = '1' on node 2
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.af_dc (pk, ck, a, x) VALUES (0, 10, '0', '0') USING TIMESTAMP 1"), ConsistencyLevel.ALL);
            cluster.get(1).executeInternal(withKeyspace("UPDATE %s.af_dc USING TIMESTAMP 2 SET x = '1' WHERE pk = 0 AND ck = 10"));
            cluster.get(2).executeInternal(withKeyspace("UPDATE %s.af_dc USING TIMESTAMP 2 SET a = '1' WHERE pk = 0 AND ck = 10"));
            // Node 1 only: decoys sort first and match a only, full matches follow
            for (int ck = 0; ck <= 5; ck++)
            {
                cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.af_dc (pk, ck, a, x) VALUES (0, ?, '1', '0')"), ck);
                cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.af_dc (pk, ck, a, x) VALUES (0, ?, '1', '1')"), ck + 20);
            }

            String partitionRead = "SELECT ck FROM %s.af_dc WHERE pk = 0 AND x = '1' AND a = '1' ALLOW FILTERING";
            String rangeRead = "SELECT ck FROM %s.af_dc WHERE x = '1' AND a = '1' ALLOW FILTERING";
            for (String query : new String[]{ partitionRead, rangeRead })
            {
                // Control. LOCAL_QUORUM through node 1 reads node 1 alone, which filters strictly, so every
                // page is full of matches and paging goes on to the end
                assertEquals(query, ImmutableSet.of(20, 21, 22, 23, 24, 25), pagedValuesAt(cluster, query, ConsistencyLevel.LOCAL_QUORUM, 2));

                // QUORUM reads both replicas
                assertEquals(query, ImmutableSet.of(10, 20, 21, 22, 23, 24, 25), pagedValuesAt(cluster, query, ConsistencyLevel.QUORUM, 2));
            }
        }
    }

    @Test
    public void plainAndPartitionReadAlwaysSpeculatesWithData() throws Exception
    {
        // At TWO a read waits for two replicas and with ALWAYS contacts three of the four, so the coordinator
        // sends every read to one more replica than it waits for. Its own subnet keeps it clear of the shared
        // cluster, as in PagingTest.
        try (Cluster cluster = init(Cluster.build(4)
                                           .withSubnet(1)
                                           .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                                       .with(GOSSIP).with(NETWORK))
                                           .start(),
                                    4))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.af_always (pk int, ck int, a text, x text, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE' AND speculative_retry = 'ALWAYS'"));

            // One split row per node pair, so any two responders hold both halves of exactly one row
            int ck = 10;
            for (int first = 1; first <= 4; first++)
            {
                for (int second = first + 1; second <= 4; second++, ck++)
                {
                    cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.af_always (pk, ck, a, x) VALUES (0, ?, '0', '0') USING TIMESTAMP 1"), ConsistencyLevel.ALL, ck);
                    cluster.get(first).executeInternal(withKeyspace("UPDATE %s.af_always USING TIMESTAMP 2 SET x = '1' WHERE pk = 0 AND ck = ?"), ck);
                    cluster.get(second).executeInternal(withKeyspace("UPDATE %s.af_always USING TIMESTAMP 2 SET a = '1' WHERE pk = 0 AND ck = ?"), ck);
                }
            }
            for (int decoy = 0; decoy <= 5; decoy++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.af_always (pk, ck, a, x) VALUES (1, ?, '1', '0')"), ConsistencyLevel.ALL, decoy);

            long speculated = speculativeRetries(cluster, "af_always");

            // Control. Every replica keeps the decoys on a = '1'
            assertEquals(0, cluster.coordinator(1).execute(withKeyspace("SELECT ck FROM %s.af_always WHERE pk = 1 AND x = '1' AND a = '1' ALLOW FILTERING"),
                                                           ConsistencyLevel.TWO).length);

            Object[][] rows = cluster.coordinator(1).execute(withKeyspace("SELECT ck FROM %s.af_always WHERE pk = 0 AND x = '1' AND a = '1' ALLOW FILTERING"),
                                                             ConsistencyLevel.TWO);
            Set<Object> values = Arrays.stream(rows).map(row -> row[0]).collect(Collectors.toSet());
            assertFalse(values.isEmpty());
            assertTrue(values.toString(), ImmutableSet.of(10, 11, 12, 13, 14, 15).containsAll(values));

            // Control. One speculative retry per read, as for any read with ALWAYS
            assertEquals(speculated + 2, speculativeRetries(cluster, "af_always"));
        }
    }

    /**
     * pk 0 holds three rows split across a different pair of nodes each, as in plainAndSplitAcrossReplicasByFiltering,
     * after decoys that match a only. pk 1 holds the decoys only, the same on every replica.
     */
    private static void writeSplitRowsAndDecoys(String table)
    {
        for (int ck = 10; ck <= 12; ck++)
        {
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s." + table + " (pk, ck, a, x) VALUES (0, ?, '0', '0') USING TIMESTAMP 1"),
                                           ConsistencyLevel.ALL, ck);
            CLUSTER.get(ck - 9).executeInternal(withKeyspace("UPDATE %s." + table + " USING TIMESTAMP 2 SET x = '1' WHERE pk = 0 AND ck = ?"), ck);
            CLUSTER.get((ck - 9) % NODES + 1).executeInternal(withKeyspace("UPDATE %s." + table + " USING TIMESTAMP 2 SET a = '1' WHERE pk = 0 AND ck = ?"), ck);
        }

        for (int pk = 0; pk <= 1; pk++)
            for (int ck = 0; ck <= 5; ck++)
                CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s." + table + " (pk, ck, a, x) VALUES (?, ?, '1', '0')"),
                                               ConsistencyLevel.ALL, pk, ck);
    }

    private static void assertSerialReadsKeepApacheBehaviour(String query, String decoys)
    {
        assertEquals(0, CLUSTER.coordinator(1).execute(withKeyspace(query), ConsistencyLevel.SERIAL).length);
        assertEquals(0, CLUSTER.coordinator(1).execute(withKeyspace(query), ConsistencyLevel.LOCAL_SERIAL).length);
        assertEquals(0, CLUSTER.coordinator(1).execute(withKeyspace(decoys), ConsistencyLevel.SERIAL).length);
    }

    private static long speculativeRetries(Cluster cluster, String table)
    {
        return cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).metric.speculativeRetries.getCount());
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
    private static Set<Object> pagedValuesAt(Cluster cluster, String select, ConsistencyLevel cl, int pageSize)
    {
        Set<Object> values = new HashSet<>();
        int count = 0;
        Iterator<Object[]> pages = cluster.coordinator(1).executeWithPaging(withKeyspace(select), cl, pageSize);
        while (pages.hasNext())
        {
            values.add(pages.next()[0]);
            count++;
        }
        assertEquals("duplicate rows in " + values, values.size(), count);
        return values;
    }
}
