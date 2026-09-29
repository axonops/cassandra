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
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
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
 * OR queries on a 3 node, RF 3 cluster read at ALL and QUORUM, including split matches under several
 * disjunctions, the same splits on the filtering path when no index serves the disjunction, and IN
 * re-checked on merged rows: replica filtering protection divergence
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

        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.analyzed WHERE (body MATCH 'quick' AND v = 7) OR w = 5"), ConsistencyLevel.ALL),
                   row(0));

        // A stale analyzed match overwritten on another replica must be dropped by the
        // coordinator re-analyzing the merged newest value
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.analyzed USING TIMESTAMP 1 SET body = 'quick brown fox', v = 7 WHERE pk = 10"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.analyzed USING TIMESTAMP 2 SET body = 'lazy dog' WHERE pk = 10"));

        assertEquals(0, countAtAll("SELECT pk FROM %s.analyzed WHERE pk = 10 AND ((body MATCH 'quick' AND v = 7) OR w = 5) ALLOW FILTERING"));
    }

    @Test
    public void disjunctionsSplitAcrossReplicas()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.split (pk int, ck int, a text, b text, c text, d text, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'"));
        for (String column : new String[]{ "a", "b", "c", "d" })
            CLUSTER.schemaChange(withKeyspace("CREATE INDEX split_" + column + "_idx ON %s.split(" + column + ") USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // The three split rows share one partition, so a QUORUM read contacts one replica pair.
        // Each row holds its newest a = '1' on one node and its newest c = '1' on the next node, so
        // no replica matches both disjunctions on its own: ck 0 splits across nodes 1 and 2, ck 1
        // across nodes 2 and 3, ck 2 across nodes 3 and 1. Whatever pair QUORUM contacts, exactly
        // one row has both halves inside it.
        for (int ck = 0; ck <= 2; ck++)
        {
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.split (pk, ck, a, b, c, d) VALUES (0, ?, '0', '0', '0', '0') USING TIMESTAMP 1"),
                                           ConsistencyLevel.ALL, ck);
            CLUSTER.get(ck + 1).executeInternal(withKeyspace("UPDATE %s.split USING TIMESTAMP 2 SET a = '1' WHERE pk = 0 AND ck = ?"), ck);
            CLUSTER.get((ck + 1) % NODES + 1).executeInternal(withKeyspace("UPDATE %s.split USING TIMESTAMP 2 SET c = '1' WHERE pk = 0 AND ck = ?"), ck);
        }

        // Decoys every replica keeps under the union and the coordinator re-check drops
        for (int pk = 10; pk <= 15; pk++)
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.split (pk, ck, a, b, c, d) VALUES (?, 0, '1', '0', '0', '0')"),
                                           ConsistencyLevel.ALL, pk);

        // The same split under a plain AND is kept by the strictness downgrade for unrepaired matches
        String control = "SELECT ck FROM %s.split WHERE a = '1' AND c = '1'";
        assertEquals(ImmutableSet.of(0, 1, 2), valuesAt(control, ConsistencyLevel.ALL));
        assertEquals(1, CLUSTER.coordinator(1).execute(withKeyspace(control), ConsistencyLevel.QUORUM).length);

        String query = "SELECT ck FROM %s.split WHERE (a = '1' OR b = '2') AND (c = '1' OR d = '2')";
        assertEquals(ImmutableSet.of(0, 1, 2), valuesAt(query, ConsistencyLevel.ALL));
        assertEquals(1, CLUSTER.coordinator(1).execute(withKeyspace(query), ConsistencyLevel.QUORUM).length);

        for (int pageSize : new int[]{ 1, 2, 100 })
        {
            Set<Object> paged = new HashSet<>();
            int count = 0;
            Iterator<Object[]> pages = CLUSTER.coordinator(1).executeWithPaging(withKeyspace(query), ConsistencyLevel.ALL, pageSize);
            while (pages.hasNext())
            {
                paged.add(pages.next()[0]);
                count++;
            }
            assertEquals("page size " + pageSize, ImmutableSet.of(0, 1, 2), paged);
            assertEquals("page size " + pageSize, 3, count);
        }
    }

    @Test
    public void unindexedConjunctNextToSplitDisjunction()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.unindexed_split (pk int, ck int, a text, b text, x text, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX unindexed_split_a_idx ON %s.unindexed_split(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX unindexed_split_b_idx ON %s.unindexed_split(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // One partition, three rows each split across a different pair of nodes, as in disjunctionsSplitAcrossReplicas
        for (int ck = 0; ck <= 2; ck++)
        {
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.unindexed_split (pk, ck, a, b, x) VALUES (0, ?, '0', '0', '0') USING TIMESTAMP 1"),
                                           ConsistencyLevel.ALL, ck);
            CLUSTER.get(ck + 1).executeInternal(withKeyspace("UPDATE %s.unindexed_split USING TIMESTAMP 2 SET x = '1' WHERE pk = 0 AND ck = ?"), ck);
            CLUSTER.get((ck + 1) % NODES + 1).executeInternal(withKeyspace("UPDATE %s.unindexed_split USING TIMESTAMP 2 SET a = '1' WHERE pk = 0 AND ck = ?"), ck);
        }

        // The ONE assertions are controls only: the coordinator re-check runs at ONE too
        String control = "SELECT ck FROM %s.unindexed_split WHERE x = '1' AND a = '1' ALLOW FILTERING";
        assertEquals(ImmutableSet.of(0, 1, 2), valuesAt(control, ConsistencyLevel.ALL));
        assertEquals(1, CLUSTER.coordinator(1).execute(withKeyspace(control), ConsistencyLevel.QUORUM).length);
        assertEquals(0, CLUSTER.coordinator(1).execute(withKeyspace(control), ConsistencyLevel.ONE).length);

        String query = "SELECT ck FROM %s.unindexed_split WHERE x = '1' AND (a = '1' OR b = '2') ALLOW FILTERING";
        assertEquals(ImmutableSet.of(0, 1, 2), valuesAt(query, ConsistencyLevel.ALL));
        assertEquals(1, CLUSTER.coordinator(1).execute(withKeyspace(query), ConsistencyLevel.QUORUM).length);
        assertEquals(0, CLUSTER.coordinator(1).execute(withKeyspace(query), ConsistencyLevel.ONE).length);
    }

    @Test
    public void splitDisjunctionAfterRepair()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.repaired_split (pk int PRIMARY KEY, level text, a text, b text, x text) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX repaired_split_level_idx ON %s.repaired_split(level) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX repaired_split_a_idx ON %s.repaired_split(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX repaired_split_b_idx ON %s.repaired_split(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // level is repaired on every replica, so its match never marks the read as holding
        // unrepaired matches
        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.repaired_split (pk, level) VALUES (0, '3')"), ConsistencyLevel.ALL);
        CLUSTER.forEach(instance -> instance.flush(KEYSPACE));
        CLUSTER.get(1).nodetoolResult("repair", KEYSPACE, "repaired_split").asserts().success();

        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.repaired_split SET x = '1' WHERE pk = 0"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.repaired_split SET a = '1' WHERE pk = 0"));

        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.repaired_split WHERE level = '3' AND x = '1' AND a = '1' ALLOW FILTERING"),
                                                  ConsistencyLevel.ALL),
                   row(0));
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.repaired_split WHERE level = '3' AND x = '1' AND (a = '1' OR b = '2') ALLOW FILTERING"),
                                                  ConsistencyLevel.ALL),
                   row(0));
    }

    @Test
    public void analyzedSplitDisjunctionsDropStaleMatch()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.analyzed_split (pk int PRIMARY KEY, body text, b text, c text, d text) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX analyzed_split_body_idx ON %s.analyzed_split(body) USING 'sai' " +
                                          "WITH OPTIONS = { 'index_analyzer' : 'standard' }"));
        for (String column : new String[]{ "b", "c", "d" })
            CLUSTER.schemaChange(withKeyspace("CREATE INDEX analyzed_split_" + column + "_idx ON %s.analyzed_split(" + column + ") USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // pk 0 matches only on the merged row
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.analyzed_split SET body = 'quick fox' WHERE pk = 0"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.analyzed_split SET c = '1' WHERE pk = 0"));
        // pk 10 matches only on node 1's stale data, the merged row has c = '9'
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.analyzed_split USING TIMESTAMP 1 SET body = 'quick fox', c = '1' WHERE pk = 10"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.analyzed_split USING TIMESTAMP 2 SET c = '9' WHERE pk = 10"));

        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.analyzed_split WHERE (body MATCH 'quick' OR b = '2') AND (c = '1' OR d = '2')"),
                                                  ConsistencyLevel.ALL),
                   row(0));
    }

    @Test
    public void inIsRecheckedOnMergedRowsUnderDisjunction()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.in_recheck (pk int, ck int, t text, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX in_recheck_t_idx ON %s.in_recheck(t) USING 'sai' WITH OPTIONS = { 'case_sensitive' : false }"));
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.in_recheck_plain (pk int, ck int, t text, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX in_recheck_plain_t_idx ON %s.in_recheck_plain(t) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // One partition. Each of ck 0 to 2 has a stale t = 'x' on one node and the newest t = 'y',
        // which the IN rejects, on the other two, so every replica pair holds a stale replica for
        // some row. ck 20 is 'x' everywhere.
        for (String table : new String[]{ "in_recheck", "in_recheck_plain" })
        {
            for (int ck = 0; ck <= 2; ck++)
            {
                for (int node = 1; node <= NODES; node++)
                {
                    String value = node == ck + 1 ? "x" : "y";
                    long timestamp = node == ck + 1 ? 1 : 2;
                    CLUSTER.get(node).executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk, ck, t) VALUES (0, ?, ?) USING TIMESTAMP " + timestamp), ck, value);
                }
            }
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s." + table + " (pk, ck, t) VALUES (0, 20, 'x')"), ConsistencyLevel.ALL);
        }

        for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.QUORUM, ConsistencyLevel.ALL })
        {
            // Without an analyzer the coordinator re-check applies the whole row filter, IN included
            assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT ck FROM %s.in_recheck_plain WHERE t IN ('a', 'x') AND (t = 'x' OR t = 'y') ALLOW FILTERING"), cl),
                       row(20));
            assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT ck FROM %s.in_recheck WHERE t IN ('a', 'x') AND (t = 'x' OR t = 'y') ALLOW FILTERING"), cl),
                       row(20));
            // The disjunction matches through the index analyzer, the IN compares raw values
            assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT ck FROM %s.in_recheck WHERE t IN ('a', 'x') AND (t = 'X' OR t = 'Y') ALLOW FILTERING"), cl),
                       row(20));
        }
    }

    @Test
    public void staticConjunctNextToSplitDisjunction()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.static_split (pk int, ck int, s text static, a text, b text, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX static_split_a_idx ON %s.static_split(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX static_split_b_idx ON %s.static_split(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // pk 0 matches only on the merged partition
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.static_split SET s = '1' WHERE pk = 0"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.static_split SET a = '1' WHERE pk = 0 AND ck = 1"));
        // pk 1 matches the disjunction but not the static, on every replica: replicas keep it under
        // the union and the coordinator re-check drops it
        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.static_split (pk, ck, s, a) VALUES (1, 1, '0', '1')"), ConsistencyLevel.ALL);

        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk, ck FROM %s.static_split WHERE s = '1' AND (a = '1' OR b = '2') ALLOW FILTERING"),
                                                  ConsistencyLevel.ALL),
                   row(0, 1));
    }

    @Test
    public void nestedDisjunctionsSplitAcrossReplicas()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.nested_split (pk int PRIMARY KEY, a text, b text, c text, d text, e text, f text) WITH read_repair = 'NONE'"));
        for (String column : new String[]{ "a", "b", "c", "d", "e", "f" })
            CLUSTER.schemaChange(withKeyspace("CREATE INDEX nested_split_" + column + "_idx ON %s.nested_split(" + column + ") USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        for (int pk = 0; pk <= 1; pk++)
        {
            CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.nested_split SET a = '1' WHERE pk = ?"), pk);
            CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.nested_split SET c = '1' WHERE pk = ?"), pk);
        }
        CLUSTER.get(3).executeInternal(withKeyspace("UPDATE %s.nested_split SET e = '1' WHERE pk = 1"));

        assertEquals(ImmutableSet.of(0, 1), valuesAt("SELECT pk FROM %s.nested_split WHERE ((a = '1' OR b = '2') AND (c = '1' OR d = '2')) OR e = '5'",
                                                  ConsistencyLevel.ALL));
        assertEquals(ImmutableSet.of(1), valuesAt("SELECT pk FROM %s.nested_split WHERE (a = '1' OR b = '2') AND (c = '1' OR d = '2') AND (e = '1' OR f = '2')",
                                               ConsistencyLevel.ALL));
    }

    @Test
    public void disjunctionsSplitAcrossReplicasByFiltering()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.filter_split (pk int, ck int, a text, b text, c text, d text, x text, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'"));

        // The three split rows share one partition, so a QUORUM read contacts one replica pair.
        // Each row holds its newest x = '1' and c = '1' on one node and its newest a = '1' on the next
        // node, so no replica matches a whole query on its own: ck 10 splits across nodes 1 and 2,
        // ck 11 across nodes 2 and 3, ck 12 across nodes 3 and 1. Whatever pair QUORUM contacts,
        // exactly one row has both halves inside it.
        for (int ck = 10; ck <= 12; ck++)
        {
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.filter_split (pk, ck, a, b, c, d, x) VALUES (0, ?, '0', '0', '0', '0', '0') USING TIMESTAMP 1"),
                                           ConsistencyLevel.ALL, ck);
            CLUSTER.get(ck - 9).executeInternal(withKeyspace("UPDATE %s.filter_split USING TIMESTAMP 2 SET x = '1', c = '1' WHERE pk = 0 AND ck = ?"), ck);
            CLUSTER.get((ck - 9) % NODES + 1).executeInternal(withKeyspace("UPDATE %s.filter_split USING TIMESTAMP 2 SET a = '1' WHERE pk = 0 AND ck = ?"), ck);
        }

        // Decoys sort first. Every replica keeps them on x = '1' and the coordinator re-check drops
        // them, so replica pages fill with rows the coordinator discards and short reads follow
        for (int ck = 0; ck <= 5; ck++)
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.filter_split (pk, ck, a, b, c, d, x) VALUES (0, ?, '0', '0', '0', '0', '1')"),
                                           ConsistencyLevel.ALL, ck);

        // Control. Plain AND with ALLOW FILTERING keeps the Apache behaviour. Each replica checks the
        // whole AND on its own copy, so a row whose matching values sit on different replicas is not
        // returned. The fork leaves it unchanged, and the assertion makes any change a visible decision.
        String stock = "SELECT ck FROM %s.filter_split WHERE x = '1' AND a = '1' ALLOW FILTERING";
        assertEquals(0, CLUSTER.coordinator(1).execute(withKeyspace(stock), ConsistencyLevel.ALL).length);
        assertEquals(0, CLUSTER.coordinator(1).execute(withKeyspace(stock), ConsistencyLevel.QUORUM).length);

        // Control. The coordinator re-check runs at ONE too
        String query = "SELECT ck FROM %s.filter_split WHERE x = '1' AND (a = '1' OR b = '2') ALLOW FILTERING";
        assertEquals(0, CLUSTER.coordinator(1).execute(withKeyspace(query), ConsistencyLevel.ONE).length);

        String disjunctions = "SELECT ck FROM %s.filter_split WHERE (a = '1' OR b = '2') AND (c = '1' OR d = '2') ALLOW FILTERING";
        for (String select : new String[]{ query, disjunctions })
        {
            assertEquals(select, ImmutableSet.of(10, 11, 12), valuesAt(select, ConsistencyLevel.ALL));
            assertEquals(select, 1, CLUSTER.coordinator(1).execute(withKeyspace(select), ConsistencyLevel.QUORUM).length);
        }

        for (int pageSize : new int[]{ 1, 2, 100 })
        {
            Set<Object> paged = new HashSet<>();
            int count = 0;
            Iterator<Object[]> pages = CLUSTER.coordinator(1).executeWithPaging(withKeyspace(query), ConsistencyLevel.ALL, pageSize);
            while (pages.hasNext())
            {
                paged.add(pages.next()[0]);
                count++;
            }
            assertEquals("page size " + pageSize, ImmutableSet.of(10, 11, 12), paged);
            assertEquals("page size " + pageSize, 3, count);
        }

        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT ck FROM %s.filter_split WHERE x = '1' AND (a = '1' OR b = '2') LIMIT 2 ALLOW FILTERING"),
                                                  ConsistencyLevel.ALL),
                   row(10), row(11));
    }

    @Test
    public void staticConjunctSplitFromDisjunctionByFiltering()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.filter_static (pk int, ck int, s text static, t text static, a text, b text, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'"));

        // Static cells belong to the partition, so each case is its own partition, read at ALL.
        // pk 0 matches only on the merged partition: node 1 holds s, node 2 holds the row
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.filter_static SET s = '1' WHERE pk = 0"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.filter_static SET a = '1' WHERE pk = 0 AND ck = 1"));
        // pk 1 matches s and neither disjunct on every replica, so it is never a result
        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.filter_static (pk, ck, s, a) VALUES (1, 1, '1', '0')"), ConsistencyLevel.ALL);
        // pk 2: node 1 holds the newest statics and no rows, nodes 2 and 3 hold the row under older statics
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.filter_static USING TIMESTAMP 2 SET s = '1', t = '1' WHERE pk = 2"));
        for (int node = 2; node <= NODES; node++)
            CLUSTER.get(node).executeInternal(withKeyspace("INSERT INTO %s.filter_static (pk, ck, s, t, a) VALUES (2, 1, '0', '0', '0') USING TIMESTAMP 1"));
        // pk 3 has matching statics and no rows on any replica
        CLUSTER.coordinator(1).execute(withKeyspace("UPDATE %s.filter_static SET s = '1', t = '1' WHERE pk = 3"), ConsistencyLevel.ALL);
        // pk 4: nodes 2 and 3 hold both rows under older statics that still hold every restricted
        // column, so they are not silent on the static row. Node 1 holds only the newest statics.
        for (int node = 2; node <= NODES; node++)
        {
            CLUSTER.get(node).executeInternal(withKeyspace("INSERT INTO %s.filter_static (pk, ck, s, t, a) VALUES (4, 1, '0', '0', '0') USING TIMESTAMP 1"));
            CLUSTER.get(node).executeInternal(withKeyspace("INSERT INTO %s.filter_static (pk, ck, a) VALUES (4, 2, '1') USING TIMESTAMP 1"));
        }
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.filter_static USING TIMESTAMP 2 SET s = '1', t = '1' WHERE pk = 4"));

        // Control. Plain AND with ALLOW FILTERING keeps the Apache behaviour. Each replica checks the
        // whole AND on its own copy, so a row whose matching values sit on different replicas is not
        // returned. The fork leaves it unchanged, and the assertion makes any change a visible decision.
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk, ck FROM %s.filter_static WHERE s = '1' AND a = '1' ALLOW FILTERING"),
                                                  ConsistencyLevel.ALL));

        // (0, 1) comes back through the row fetched from node 1, which also returns node 1's static row
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk, ck FROM %s.filter_static WHERE s = '1' AND (a = '1' OR b = '2') ALLOW FILTERING"),
                                                  ConsistencyLevel.ALL),
                   row(0, 1), row(4, 2));

        // (2, 1) needs node 1 to return pk 2 with its static row and no rows. (4, 1) needs the whole
        // partition read from nodes 2 and 3, whose statics are older than node 1's. pk 3 has no row
        // to return, as the doc states for a partition holding only a matching static row.
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk, ck FROM %s.filter_static WHERE s = '1' AND (t = '1' OR a = '1') ALLOW FILTERING"),
                                                  ConsistencyLevel.ALL),
                   row(0, 1), row(2, 1), row(4, 1), row(4, 2));

        // A disjunction with a static leaf and no AND over several columns. Node 1 holds s = '1' with no rows for
        // pk 0, 2 and 4, and the other replicas hold the rows with no static row or an older one.
        // pk 1 matches on every replica. pk 3 has no row to return.
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk, ck FROM %s.filter_static WHERE s = '1' OR b = '2' ALLOW FILTERING"),
                                                  ConsistencyLevel.ALL),
                   row(1, 1), row(0, 1), row(2, 1), row(4, 1), row(4, 2));
    }

    @Test
    public void nestedDisjunctionsSplitAcrossReplicasByFiltering()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.filter_nested (pk int, ck int, a text, b text, c text, d text, e text, x text, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'"));

        // One partition, three rows each split across a different pair of nodes, as in
        // disjunctionsSplitAcrossReplicasByFiltering. Every replica holds x = '1' for every row.
        for (int ck = 10; ck <= 12; ck++)
        {
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.filter_nested (pk, ck, a, b, c, d, e, x) VALUES (0, ?, '0', '0', '0', '0', '0', '1') USING TIMESTAMP 1"),
                                           ConsistencyLevel.ALL, ck);
            CLUSTER.get(ck - 9).executeInternal(withKeyspace("UPDATE %s.filter_nested USING TIMESTAMP 2 SET a = '1' WHERE pk = 0 AND ck = ?"), ck);
            CLUSTER.get((ck - 9) % NODES + 1).executeInternal(withKeyspace("UPDATE %s.filter_nested USING TIMESTAMP 2 SET c = '1' WHERE pk = 0 AND ck = ?"), ck);
        }

        // Control. A conjunction on one column is evaluated in full on each replica
        assertEquals(ImmutableSet.of(10, 11, 12), valuesAt("SELECT ck FROM %s.filter_nested WHERE (a > '0' AND a < '2') OR e = '5' ALLOW FILTERING",
                                                           ConsistencyLevel.ALL));

        // The last two put the AND branch under OR under a conjunction with x. In the first of them x
        // is the conjunct a replica checks, in the second the AND branch is.
        for (String select : new String[]{ "SELECT ck FROM %s.filter_nested WHERE (a = '1' AND c = '1') OR e = '5' ALLOW FILTERING",
                                           "SELECT ck FROM %s.filter_nested WHERE ((a = '1' OR b = '2') AND (c = '1' OR d = '2')) OR e = '5' ALLOW FILTERING",
                                           "SELECT ck FROM %s.filter_nested WHERE ((a = '1' AND c = '1') OR e = '5') AND x = '1' ALLOW FILTERING",
                                           "SELECT ck FROM %s.filter_nested WHERE ((a = '1' AND c = '1') OR e = '5') AND (x = '1' OR b = '2') ALLOW FILTERING" })
        {
            assertEquals(select, ImmutableSet.of(10, 11, 12), valuesAt(select, ConsistencyLevel.ALL));
            assertEquals(select, 1, CLUSTER.coordinator(1).execute(withKeyspace(select), ConsistencyLevel.QUORUM).length);
        }
    }

    @Test
    public void staticDivergenceUnderDisjunctionOnIndexPath()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.sai_static (pk int, ck int, s text static, t text static, a text, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX sai_static_t_idx ON %s.sai_static(t) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX sai_static_a_idx ON %s.sai_static(a) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // As pk 4 in staticConjunctSplitFromDisjunctionByFiltering: nodes 2 and 3 hold both rows under
        // older statics that still hold every restricted column, node 1 holds only the newest statics
        for (int node = 2; node <= NODES; node++)
        {
            CLUSTER.get(node).executeInternal(withKeyspace("INSERT INTO %s.sai_static (pk, ck, s, t, a) VALUES (4, 1, '0', '0', '0') USING TIMESTAMP 1"));
            CLUSTER.get(node).executeInternal(withKeyspace("INSERT INTO %s.sai_static (pk, ck, a) VALUES (4, 2, '1') USING TIMESTAMP 1"));
        }
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.sai_static USING TIMESTAMP 2 SET s = '1', t = '1' WHERE pk = 4"));

        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT pk, ck FROM %s.sai_static WHERE s = '1' AND (t = '1' OR a = '1') ALLOW FILTERING"),
                                                  ConsistencyLevel.ALL),
                   row(4, 1), row(4, 2));
    }

    @Test
    public void plainAndStaticDivergenceKeepsStockProtectionReads()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.stock_static (pk int, ck int, s text static, t text static, v text, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX stock_static_s_idx ON %s.stock_static(s) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX stock_static_t_idx ON %s.stock_static(t) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // Every replica's static row differs from the merged one on s or t, and every replica returns
        // every row under the index union for unrepaired matches, so no replica is silent on any row
        for (int ck = 1; ck <= 3; ck++)
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.stock_static (pk, ck, s, t, v) VALUES (0, ?, '0', '0', '0') USING TIMESTAMP 1"),
                                           ConsistencyLevel.ALL, ck);
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.stock_static USING TIMESTAMP 2 SET s = '1' WHERE pk = 0"));
        CLUSTER.get(3).executeInternal(withKeyspace("UPDATE %s.stock_static USING TIMESTAMP 2 SET s = '1' WHERE pk = 0"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.stock_static USING TIMESTAMP 2 SET t = '1' WHERE pk = 0"));

        long protectionReads = CLUSTER.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                                           .getColumnFamilyStore("stock_static")
                                                                           .metric.replicaFilteringProtectionRequests.getCount());

        // Control
        assertEquals(ImmutableSet.of(1, 2, 3), valuesAt("SELECT ck FROM %s.stock_static WHERE s = '1' AND t = '1'", ConsistencyLevel.ALL));

        // Without OR the coordinator reads no extra rows here, as Apache does
        assertEquals(protectionReads, (long) CLUSTER.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                                                         .getColumnFamilyStore("stock_static")
                                                                                         .metric.replicaFilteringProtectionRequests.getCount()));
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
