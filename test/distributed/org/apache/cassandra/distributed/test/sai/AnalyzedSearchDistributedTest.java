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

import java.net.InetAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.ClusterVersionGate;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * The analyzed operators on a 3 node, RF 2 cluster at QUORUM: plain analyzed counts, and replica
 * divergence cases where replica filtering protection must re-apply the analyzer on the
 * coordinator to keep merged results correct.
 */
public class AnalyzedSearchDistributedTest extends TestBaseImpl
{
    private static final int NODES = 3;

    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws Exception
    {
        // The analyzed operators are refused unless every node advertises the fork messaging
        // version, which nodes in a before-5 storage compatibility mode do not
        CLUSTER = init(Cluster.build(NODES)
                              .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                          .set("storage_compatibility_mode", "NONE")
                                                          .with(GOSSIP).with(NETWORK))
                              .start(),
                       2);
        waitForForkVersionAgreement();
    }

    @AfterClass
    public static void shutDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void analyzedCountsAtQuorum()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.docs (pk int PRIMARY KEY, body text) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX docs_body_idx ON %s.docs(body) USING 'sai' " +
                                          "WITH OPTIONS = { 'index_analyzer' : 'english' }"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        for (int pk = 0; pk < 20; pk++)
        {
            String body = pk % 2 == 0 ? "the quick brown fox" : "the lazy dog sleeps";
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.docs (pk, body) VALUES (?, ?)"),
                                           ConsistencyLevel.ALL, pk, body);

            // half of the data comes from sstables, half stays in memtables
            if (pk == 9)
                CLUSTER.forEach(instance -> instance.flush(KEYSPACE));
        }

        assertEquals(10, quorumCount("SELECT pk FROM %s.docs WHERE body MATCH 'quick fox'"));
        assertEquals(10, quorumCount("SELECT pk FROM %s.docs WHERE body MATCH 'dog'"));
        assertEquals(0, quorumCount("SELECT pk FROM %s.docs WHERE body MATCH 'quick dog'"));
        assertEquals(10, quorumCount("SELECT pk FROM %s.docs WHERE body PHRASE 'quick brown fox'"));
        assertEquals(0, quorumCount("SELECT pk FROM %s.docs WHERE body PHRASE 'brown quick'"));
        // the english analyzer drops the stopword but preserves its gap, so 'lazy dog' is adjacent
        assertEquals(10, quorumCount("SELECT pk FROM %s.docs WHERE body PHRASE 'lazy dog'"));
    }

    @Test
    public void divergedReplicasAreResolvedByReanalysis()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.diverged (pk int PRIMARY KEY, body text) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX diverged_body_idx ON %s.diverged(body) USING 'sai' " +
                                          "WITH OPTIONS = { 'index_analyzer' : 'standard' }"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        int pk = 0;
        List<Integer> replicas = replicaNodesFor(pk);
        assertEquals(2, replicas.size());

        // One replica indexed a stale 'quick brown fox', the other holds the newer 'lazy dog'
        CLUSTER.get(replicas.get(0)).executeInternal(withKeyspace("INSERT INTO %s.diverged (pk, body) VALUES (?, ?) USING TIMESTAMP 1"),
                                                     pk, "quick brown fox");
        CLUSTER.get(replicas.get(1)).executeInternal(withKeyspace("INSERT INTO %s.diverged (pk, body) VALUES (?, ?) USING TIMESTAMP 2"),
                                                     pk, "lazy dog");

        // The stale index match must be dropped by the coordinator re-analyzing the merged newest value
        assertEquals(0, quorumCount("SELECT pk FROM %s.diverged WHERE body MATCH 'fox'"));
        assertEquals(0, quorumCount("SELECT pk FROM %s.diverged WHERE body PHRASE 'quick brown'"));

        // The newest value matches even though only one replica has it indexed
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.diverged WHERE body MATCH 'lazy dog'"), ConsistencyLevel.QUORUM),
                   row(pk));
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.diverged WHERE body PHRASE 'lazy dog'"), ConsistencyLevel.QUORUM),
                   row(pk));
    }

    @Test
    public void partialRowsAcrossReplicasMergeCorrectly()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partial (pk int PRIMARY KEY, a text, b text) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX partial_a_idx ON %s.partial(a) USING 'sai' " +
                                          "WITH OPTIONS = { 'index_analyzer' : 'standard' }"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX partial_b_idx ON %s.partial(b) USING 'sai' " +
                                          "WITH OPTIONS = { 'index_analyzer' : 'standard' }"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        int pk = 0;
        List<Integer> replicas = replicaNodesFor(pk);
        assertEquals(2, replicas.size());

        // Each replica holds a different partial version of the same row
        CLUSTER.get(replicas.get(0)).executeInternal(withKeyspace("UPDATE %s.partial SET a = ? WHERE pk = ?"),
                                                     "quick brown fox", pk);
        CLUSTER.get(replicas.get(1)).executeInternal(withKeyspace("UPDATE %s.partial SET b = ? WHERE pk = ?"),
                                                     "lazy sleepy dog", pk);

        // Neither replica matches both predicates locally; the merged row does, and the
        // coordinator's re-analysis must accept it
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.partial WHERE a MATCH 'quick' AND b MATCH 'dog'"), ConsistencyLevel.QUORUM),
                   row(pk));
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.partial WHERE a PHRASE 'brown fox' AND b PHRASE 'sleepy dog'"), ConsistencyLevel.QUORUM),
                   row(pk));

        // ...and a predicate the merged row does not satisfy stays unmatched
        assertEquals(0, quorumCount("SELECT pk FROM %s.partial WHERE a MATCH 'quick' AND b MATCH 'cat'"));
        assertEquals(0, quorumCount("SELECT pk FROM %s.partial WHERE a PHRASE 'fox brown' AND b PHRASE 'sleepy dog'"));
    }

    private static int quorumCount(String select)
    {
        return CLUSTER.coordinator(1).execute(withKeyspace(select), ConsistencyLevel.QUORUM).length;
    }

    /**
     * @return the node ids of the replicas of the given partition key, in ring order
     */
    private static List<Integer> replicaNodesFor(int pk)
    {
        List<String> endpoints = CLUSTER.get(1).callOnInstance(() -> StorageService.instance
                                                                     .getNaturalEndpoints(KEYSPACE, Int32Type.instance.decompose(pk))
                                                                     .stream()
                                                                     .map(InetAddress::getHostAddress)
                                                                     .collect(Collectors.toList()));

        Set<String> endpointSet = new HashSet<>(endpoints);
        return CLUSTER.stream()
                      .filter(instance -> endpointSet.contains(instance.config().broadcastAddress().getAddress().getHostAddress()))
                      .map(instance -> instance.config().num())
                      .collect(Collectors.toList());
    }

    /**
     * The analyzed operators are gated on every live peer advertising the fork messaging version,
     * which nodes learn through the handshake and gossip. Waits until every node's gate opens so
     * the tests never race that propagation.
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
