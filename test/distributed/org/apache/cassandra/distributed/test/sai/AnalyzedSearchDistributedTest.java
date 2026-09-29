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
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.ClusterVersionGate;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexQueryPlan;
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
    public void divergedMapKeysAreResolvedByReanalysis()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.diverged_keys (pk int PRIMARY KEY, attrs map<text, text>) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX diverged_keys_idx ON %s.diverged_keys(KEYS(attrs)) USING 'sai' " +
                                          "WITH OPTIONS = { 'index_analyzer' : 'standard' }"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX diverged_values_idx ON %s.diverged_keys(VALUES(attrs)) USING 'sai' " +
                                          "WITH OPTIONS = { 'index_analyzer' : 'standard' }"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        int pk = 0;
        List<Integer> replicas = replicaNodesFor(pk);
        assertEquals(2, replicas.size());

        // One replica indexed a stale key 'quick brown fox'. The other holds the newer map, whose
        // only key is 'lazy dog' and whose value repeats the stale words.
        CLUSTER.get(replicas.get(0)).executeInternal(withKeyspace("INSERT INTO %s.diverged_keys (pk, attrs) VALUES (?, {'quick brown fox': 'a'}) USING TIMESTAMP 1"),
                                                     pk);
        CLUSTER.get(replicas.get(1)).executeInternal(withKeyspace("INSERT INTO %s.diverged_keys (pk, attrs) VALUES (?, {'lazy dog': 'quick brown fox'}) USING TIMESTAMP 2"),
                                                     pk);

        // The stale key match must be dropped by the coordinator re-analyzing the keys of the merged map
        assertEquals(0, quorumCount("SELECT pk FROM %s.diverged_keys WHERE attrs MATCH KEY 'fox'"));
        assertEquals(0, quorumCount("SELECT pk FROM %s.diverged_keys WHERE attrs PHRASE KEY 'quick brown'"));

        // The newest key matches even though only one replica has it indexed
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.diverged_keys WHERE attrs MATCH KEY 'lazy dog'"), ConsistencyLevel.QUORUM),
                   row(pk));
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.diverged_keys WHERE attrs PHRASE KEY 'lazy dog'"), ConsistencyLevel.QUORUM),
                   row(pk));

        // The same words in the newest value are found through the index on the values
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.diverged_keys WHERE attrs MATCH 'fox'"), ConsistencyLevel.QUORUM),
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

    @Test
    public void severalRelationsAreResolvedByReanalysis()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.diverged_several (pk int PRIMARY KEY, body text) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX diverged_several_idx ON %s.diverged_several(body) USING 'sai' " +
                                          "WITH OPTIONS = { 'index_analyzer' : 'standard' }"));
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.diverged_several_static (pk int, ck int, s text static, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX diverged_several_static_idx ON %s.diverged_several_static(s) USING 'sai' " +
                                          "WITH OPTIONS = { 'index_analyzer' : 'standard' }"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        int pk = 0;
        List<Integer> replicas = replicaNodesFor(pk);
        assertEquals(2, replicas.size());

        // One replica indexed a stale 'quick brown fox', the other holds the newer 'lazy brown dog'
        CLUSTER.get(replicas.get(0)).executeInternal(withKeyspace("INSERT INTO %s.diverged_several (pk, body) VALUES (?, ?) USING TIMESTAMP 1"),
                                                     pk, "quick brown fox");
        CLUSTER.get(replicas.get(1)).executeInternal(withKeyspace("INSERT INTO %s.diverged_several (pk, body) VALUES (?, ?) USING TIMESTAMP 2"),
                                                     pk, "lazy brown dog");
        CLUSTER.get(replicas.get(0)).executeInternal(withKeyspace("INSERT INTO %s.diverged_several_static (pk, ck, s) VALUES (?, 1, ?) USING TIMESTAMP 1"),
                                                     pk, "quick brown fox");
        CLUSTER.get(replicas.get(1)).executeInternal(withKeyspace("INSERT INTO %s.diverged_several_static (pk, ck, s) VALUES (?, 1, ?) USING TIMESTAMP 2"),
                                                     pk, "lazy brown dog");

        // Every relation is checked again on the merged newest value
        assertEquals(0, quorumCount("SELECT pk FROM %s.diverged_several WHERE body MATCH 'brown' AND body PHRASE 'quick brown'"));
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.diverged_several WHERE body MATCH 'lazy' AND body PHRASE 'brown dog'"), ConsistencyLevel.QUORUM),
                   row(pk));

        // Two relations on a static column make the replicas filter non strictly
        assertEquals(0, quorumCount("SELECT pk FROM %s.diverged_several_static WHERE s MATCH 'brown' AND s PHRASE 'quick brown'"));
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk, ck FROM %s.diverged_several_static WHERE s MATCH 'lazy' AND s PHRASE 'brown dog'"), ConsistencyLevel.QUORUM),
                   row(pk, 1));
    }

    @Test
    public void stockFilterAfterWordSearchOnMergedRows()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.logs_merged (pk int PRIMARY KEY, body text) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX logs_merged_body_idx ON %s.logs_merged(body) USING 'sai' " +
                                          "WITH OPTIONS = { 'index_analyzer' : 'standard' }"));
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.logs_merged_static (pk int, ck int, s text static, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX logs_merged_static_idx ON %s.logs_merged_static(s) USING 'sai' " +
                                          "WITH OPTIONS = { 'index_analyzer' : 'standard' }"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        List<Integer> replicas4 = replicaNodesFor(4);
        List<Integer> replicas5 = replicaNodesFor(5);
        assertEquals(2, replicas4.size());
        assertEquals(2, replicas5.size());

        // pk 4 has the stale 'Timeout' on one replica and the newer 'timeout after 30s' on the other,
        // pk 5 the other way round. Every value holds the word timeout.
        String insert = withKeyspace("INSERT INTO %s.logs_merged (pk, body) VALUES (?, ?) USING TIMESTAMP ?");
        CLUSTER.get(replicas4.get(0)).executeInternal(insert, 4, "Timeout", 1L);
        CLUSTER.get(replicas4.get(1)).executeInternal(insert, 4, "timeout after 30s", 2L);
        CLUSTER.get(replicas5.get(0)).executeInternal(insert, 5, "timeout after 30s", 1L);
        CLUSTER.get(replicas5.get(1)).executeInternal(insert, 5, "Timeout", 2L);

        // The coordinator checks the IN and the range on the merged newest value
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.logs_merged WHERE body MATCH 'timeout' AND body IN ('Timeout', 'Disk full') ALLOW FILTERING"), ConsistencyLevel.QUORUM),
                   row(5));
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.logs_merged WHERE body MATCH 'timeout' AND body > 'm' ALLOW FILTERING"), ConsistencyLevel.QUORUM),
                   row(4));

        // Two relations on a static column make the read non strict, and IN is refused on such reads
        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.logs_merged_static (pk, ck, s) VALUES (0, 1, 'Timeout')"), ConsistencyLevel.ALL);
        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.logs_merged_static (pk, ck, s) VALUES (1, 1, 'timeout after 30s')"), ConsistencyLevel.ALL);
        String staticIn = withKeyspace("SELECT pk, ck FROM %s.logs_merged_static WHERE s MATCH 'timeout' AND s IN ('Timeout', 'Disk full') ALLOW FILTERING");
        Assertions.assertThatThrownBy(() -> CLUSTER.coordinator(1).execute(staticIn, ConsistencyLevel.QUORUM))
                  .hasMessageContaining(String.format(StorageAttachedIndexQueryPlan.UNSUPPORTED_NON_STRICT_OPERATOR, Operator.IN));
        assertRows(CLUSTER.coordinator(1).execute(staticIn, ConsistencyLevel.ONE), row(0, 1));

        // A range on a static column runs non strictly, with the intersect filtering guardrail warning
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk, ck FROM %s.logs_merged_static WHERE s MATCH 'timeout' AND s > 'm' ALLOW FILTERING"), ConsistencyLevel.QUORUM),
                   row(1, 1));
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
