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

import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.ClusterVersionGate;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * OR queries on a 3 node, RF 3 cluster read at ALL: replica filtering protection divergence
 * where an AND branch under OR matches only the merged row, the strict coordinator re-filter
 * dropping stale local matches, analyzed leaves inside disjunctions, and the cluster version
 * gate refusing OR while a peer speaks the vanilla messaging version.
 */
public class DisjunctionDistributedTest extends TestBaseImpl
{
    private static final int NODES = 3;

    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws Exception
    {
        // OR queries are refused unless every node advertises the fork messaging version, which
        // nodes in a before-5 storage compatibility mode do not
        CLUSTER = init(Cluster.build(NODES)
                              .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                          .set("storage_compatibility_mode", "NONE")
                                                          .with(GOSSIP).with(NETWORK))
                              .start(),
                       NODES);
        waitForForkVersionAgreement();
    }

    @AfterClass
    public static void shutDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void disjunctionCountsAcrossTheCluster()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.basic (pk int PRIMARY KEY, a int, b int, c int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX basic_a_idx ON %s.basic(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX basic_b_idx ON %s.basic(b) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX basic_c_idx ON %s.basic(c) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        for (int pk = 0; pk < 20; pk++)
        {
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.basic (pk, a, b, c) VALUES (?, ?, ?, ?)"),
                                           ConsistencyLevel.ALL, pk, pk % 2, pk % 3, pk % 5);

            // half of the data comes from sstables, half stays in memtables
            if (pk == 9)
                CLUSTER.forEach(instance -> instance.flush(KEYSPACE));
        }

        // a = 0 matches the 10 even rows, b = 1 matches pk 1, 4, 7, 10, 13, 16, 19, and the
        // union dedups the overlap of pk 4, 10, 16
        assertEquals(14, countAtAll("SELECT pk FROM %s.basic WHERE a = 0 OR b = 1"));
        // AND under OR: the branch matches pk 0, 6, 12, 18 and c = 1 matches pk 1, 6, 11, 16
        assertEquals(7, countAtAll("SELECT pk FROM %s.basic WHERE (a = 0 AND b = 0) OR c = 1"));
        // same column disjuncts cover every row
        assertEquals(20, countAtAll("SELECT pk FROM %s.basic WHERE a = 0 OR a = 1"));
    }

    @Test
    public void andBranchUnderOrMatchesOnlyOnMergedRows()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.merged (pk int PRIMARY KEY, a int, b int, c int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX merged_a_idx ON %s.merged(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX merged_b_idx ON %s.merged(b) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX merged_c_idx ON %s.merged(c) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // With RF 3 every node replicates pk 0. Two replicas hold different partial versions
        // of the row, so no replica matches a = 1 AND b = 2 locally, only the merged row does.
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.merged SET a = 1 WHERE pk = 0"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.merged SET b = 2 WHERE pk = 0"));

        // The per node downgrade must surface the partial matches and the coordinator
        // re-filter must keep the merged row
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.merged WHERE (a = 1 AND b = 2) OR c = 3"), ConsistencyLevel.ALL),
                   row(0));

        // The other direction: a row the merged data does not match must not leak through even
        // though one replica still matches the branch on stale data
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.merged USING TIMESTAMP 1 SET a = 1, b = 2 WHERE pk = 10"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.merged USING TIMESTAMP 2 SET a = 9 WHERE pk = 10"));

        assertEquals(0, countAtAll("SELECT pk FROM %s.merged WHERE pk = 10 AND ((a = 1 AND b = 2) OR c = 3) ALLOW FILTERING"));
    }

    @Test
    public void orSubtreeUnderConjunctionMatchesOnlyOnMergedRows()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.conj (pk int PRIMARY KEY, a int, b int, c int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX conj_a_idx ON %s.conj(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX conj_b_idx ON %s.conj(b) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX conj_c_idx ON %s.conj(c) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // The conjunct and the disjunction live on different replicas, so no replica matches
        // c = 1 AND (a = 1 OR b = 2) locally, only the merged row does. The per node downgrade
        // must apply to the OR child subtree too, not only to the local expressions.
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.conj SET c = 1 WHERE pk = 0"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.conj SET a = 1 WHERE pk = 0"));

        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.conj WHERE c = 1 AND (a = 1 OR b = 2)"), ConsistencyLevel.ALL),
                   row(0));

        // The strict exclusion inverse: one replica matches fully on stale data, the merged
        // row does not, and the strict coordinator re-filter must drop it
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.conj USING TIMESTAMP 1 SET c = 1, a = 1 WHERE pk = 10"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.conj USING TIMESTAMP 2 SET c = 9 WHERE pk = 10"));

        assertEquals(0, countAtAll("SELECT pk FROM %s.conj WHERE pk = 10 AND c = 1 AND (a = 1 OR b = 2) ALLOW FILTERING"));
    }

    @Test
    public void analyzedLeavesUnderOrAreRecheckedOnMergedRows()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.analyzed (pk int PRIMARY KEY, body text, v int, w int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX analyzed_body_idx ON %s.analyzed(body) USING 'sai' " +
                                          "WITH OPTIONS = { 'index_analyzer' : 'standard' }"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX analyzed_v_idx ON %s.analyzed(v) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX analyzed_w_idx ON %s.analyzed(w) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // The analyzed leaf and the numeric leaf of one AND branch live on different replicas
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.analyzed SET body = 'quick brown fox' WHERE pk = 0"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.analyzed SET v = 7 WHERE pk = 0"));

        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.analyzed WHERE (body : 'quick' AND v = 7) OR w = 5"), ConsistencyLevel.ALL),
                   row(0));

        // A stale analyzed match overwritten on another replica must be dropped by the
        // coordinator re-analyzing the merged newest value
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.analyzed USING TIMESTAMP 1 SET body = 'quick brown fox', v = 7 WHERE pk = 10"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.analyzed USING TIMESTAMP 2 SET body = 'lazy dog' WHERE pk = 10"));

        assertEquals(0, countAtAll("SELECT pk FROM %s.analyzed WHERE pk = 10 AND ((body : 'quick' AND v = 7) OR w = 5) ALLOW FILTERING"));
    }

    @Test
    public void orIsRefusedWhileAPeerSpeaksTheVanillaVersion()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.gate (pk int PRIMARY KEY, a int, b int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX gate_a_idx ON %s.gate(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX gate_b_idx ON %s.gate(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.gate (pk, a, b) VALUES (0, 1, 2)"), ConsistencyLevel.ALL);

        // Make the coordinator believe node 2 only speaks the vanilla 5.0 messaging version
        setPeerVersionOnNode1(2, MessagingService.VERSION_50);
        try
        {
            CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.gate WHERE a = 1 OR b = 2"), ConsistencyLevel.ALL);
            fail("An OR query should be refused while a peer speaks the vanilla messaging version");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("not supported until all nodes in the cluster are running this build"));
        }
        finally
        {
            setPeerVersionOnNode1(2, MessagingService.VERSION_AXON_50);
        }

        // Once the peer advertises the fork version again the query is accepted
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.gate WHERE a = 1 OR b = 2"), ConsistencyLevel.ALL),
                   row(0));
    }

    private static void setPeerVersionOnNode1(int peer, int version)
    {
        String peerAddress = CLUSTER.get(peer).config().broadcastAddress().getAddress().getHostAddress();
        int peerPort = CLUSTER.get(peer).config().broadcastAddress().getPort();
        CLUSTER.get(1).runOnInstance(() -> {
            try
            {
                InetAddressAndPort endpoint = InetAddressAndPort.getByNameOverrideDefaults(peerAddress, peerPort);
                MessagingService.instance().versions.set(endpoint, version);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    private static int countAtAll(String select)
    {
        return CLUSTER.coordinator(1).execute(withKeyspace(select), ConsistencyLevel.ALL).length;
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
