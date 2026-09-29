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

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * The replica side of plain AND with ALLOW FILTERING at consistency levels that need reconciliation.
 */
public class PlainAndFilteringQueryTest extends CQLTester
{
    // Read command bytes of an Apache Cassandra 5.0 coordinator at QUORUM, the same at messaging versions 12 and 13,
    // for the table of plainAndFilteringCommandBytesDifferFromStockInOneBit at NOW_IN_SEC
    private static final String STOCK_PARTITION_READ = "00106f1c2a503e4b11ee9a552f7c6b8d9e036553f10009000401740176016d0173000300000173000000050001610000017600000002000400000001000001760000000400040000000a00f07ffffffff07fffffff0000000001000001010000060000";
    private static final String STOCK_RANGE_READ = "01106f1c2a503e4b11ee9a552f7c6b8d9e036553f10009000401740176016d017300030000016d000000050004000000020000017600000002000400000001000001760000000400040000000a00f07ffffffff07fffffff0000010000000880000000000000000100000008800000000000000000000101000006000000";
    private static final long NOW_IN_SEC = 1700000000L;

    @Before
    public void setup()
    {
        // Registers the virtual schema keyspace
        requireNetwork();
    }

    @Test
    public void plainAndFilteringCommandAsksForPartialMatches() throws Throwable
    {
        // RF 3 on this single node makes a QUORUM read need reconciliation. With no index the replica
        // filters its rows with the row filter alone.
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
        String table = keyspace + '.' + createTable(keyspace, "CREATE TABLE %s (pk int, ck1 int, ck2 int, a text, b text, x text, PRIMARY KEY (pk, ck1, ck2))");
        String indexed = keyspace + '.' + createTable(keyspace, "CREATE TABLE %s (pk int, ck1 int, ck2 int, a text, b text, x text, PRIMARY KEY (pk, ck1, ck2))");
        createIndex(keyspace, "CREATE INDEX ON %s(a) USING 'sai'");

        // Rows are named (pk, ck2) and all have ck1 = 0, so ck2 = 2 needs filtering and enters the row
        // filter. (0, 1) and (1, 1) match x only, (0, 2) matches a only, (0, 3) matches nothing and
        // (0, 4) matches the whole query.
        execute("INSERT INTO " + table + " (pk, ck1, ck2, x, a) VALUES (0, 0, 1, '1', '0')");
        execute("INSERT INTO " + table + " (pk, ck1, ck2, x, a) VALUES (0, 0, 2, '0', '1')");
        execute("INSERT INTO " + table + " (pk, ck1, ck2, x, a) VALUES (0, 0, 3, '0', '0')");
        execute("INSERT INTO " + table + " (pk, ck1, ck2, x, a) VALUES (0, 0, 4, '1', '1')");
        execute("INSERT INTO " + table + " (pk, ck1, ck2, x, a) VALUES (1, 0, 1, '1', '0')");

        String plainAnd = "SELECT pk, ck2 FROM " + table + " WHERE x = '1' AND a = '1' ALLOW FILTERING";
        String clustering = "SELECT pk, ck2 FROM " + table + " WHERE ck2 = 2 AND x = '1' AND a = '1' ALLOW FILTERING";

        // Control. A read at ONE needs no reconciliation and the replica stays strict
        ReadCommand one = command(plainAnd, ConsistencyLevel.ONE, FBUtilities.nowInSeconds());
        assertEquals(0x00, flags(one));
        assertEquals(ImmutableSet.of(List.of(0, 4)), kept(one, 1));

        // Control. One column cannot be split across replicas, so the filter is strict
        assertEquals(0x10, flags(command("SELECT pk, ck2 FROM " + table + " WHERE a = '1' ALLOW FILTERING", ConsistencyLevel.QUORUM, FBUtilities.nowInSeconds())));

        // Control. A read through an index keeps the index path
        ReadCommand withIndex = command("SELECT pk, ck2 FROM " + indexed + " WHERE a = '1' AND x = '1' ALLOW FILTERING", ConsistencyLevel.QUORUM, FBUtilities.nowInSeconds());
        assertNotNull(withIndex.indexQueryPlan());
        assertEquals(0x14, flags(withIndex));

        // Control. SERIAL reads keep the Apache behaviour. The coordinator does not ask replicas for partial
        // matches, because Paxos would not linearize the extra reads that follow.
        ReadCommand serial = command(plainAnd, ConsistencyLevel.SERIAL, FBUtilities.nowInSeconds());
        assertEquals(0x10, flags(serial));
        assertEquals(0x10, flags(command(plainAnd, ConsistencyLevel.LOCAL_SERIAL, FBUtilities.nowInSeconds())));
        assertEquals(ImmutableSet.of(List.of(0, 4)), kept(serial, 1));

        // Control. The coordinator re-check of the merged rows stays strict. Every row of the table stands in
        // for the merged rows.
        ReadCommand quorum = command(plainAnd, ConsistencyLevel.QUORUM, FBUtilities.nowInSeconds());
        PartitionRangeReadCommand allData = PartitionRangeReadCommand.allDataRead(quorum.metadata(), FBUtilities.nowInSeconds());
        Set<List<Integer>> merged = new HashSet<>();
        try (ReadExecutionController controller = allData.executionController();
             PartitionIterator partitions = quorum.rowFilter().filter(allData.executeInternal(controller), quorum.metadata(), FBUtilities.nowInSeconds()))
        {
            while (partitions.hasNext())
            {
                try (RowIterator partition = partitions.next())
                {
                    int pk = Int32Type.instance.compose(partition.partitionKey().getKey());
                    while (partition.hasNext())
                        merged.add(List.of(pk, Int32Type.instance.compose(partition.next().clustering().bufferAt(1))));
                }
            }
        }
        assertEquals(ImmutableSet.of(List.of(0, 4)), merged);

        // Control. A virtual table has no reconciliation, so the coordinator never looks up its keyspace,
        // which only exists in the virtual keyspace registry
        String virtual = "SELECT * FROM system_virtual_schema.columns WHERE kind = 'regular' AND type = 'text' ALLOW FILTERING";
        assertFalse(execute(virtual).isEmpty());
        SelectStatement virtualSelect = (SelectStatement) QueryProcessor.getStatement(virtual, ClientState.forInternalCalls());
        assertNotNull(virtualSelect.getQuery(QueryOptions.forInternalCalls(ConsistencyLevel.QUORUM, Collections.emptyList()), FBUtilities.nowInSeconds()));

        // A QUORUM read asks for partial matches. The replica keeps a row when a, the restricted column that
        // comes first in name byte order, matches. (0, 2) matches only a and is kept.
        assertEquals(0x90, flags(quorum));
        assertEquals(ImmutableSet.of(List.of(0, 2), List.of(0, 4)), kept(quorum, 1));

        // Clustering expressions stay strict
        assertEquals(ImmutableSet.of(List.of(0, 2)), kept(command(clustering, ConsistencyLevel.QUORUM, FBUtilities.nowInSeconds()), 1));
    }

    @Test
    public void plainAndFilteringCommandBytesDifferFromStockInOneBit() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
        String table = keyspace + '.' + createTable(keyspace, "CREATE TABLE %s (pk int, ck int, v int, t text, s set<text>, m map<text, int>, PRIMARY KEY (pk, ck)) " +
                                                              "WITH ID = 6f1c2a50-3e4b-11ee-9a55-2f7c6b8d9e03");

        // (1, 1) matches v only, (1, 2) matches s and m only. Replicas keep a row matching s or m, the
        // restricted columns that come before v in name byte order.
        execute("INSERT INTO " + table + " (pk, ck, v, s, m) VALUES (1, 1, 5, {'b'}, {'k': 1})");
        execute("INSERT INTO " + table + " (pk, ck, v, s, m) VALUES (1, 2, 20, {'a'}, {'k': 2})");

        Map<String, String> stockHex = ImmutableMap.of("pk = 1 AND v > 1 AND v < 10 AND s CONTAINS 'a'", STOCK_PARTITION_READ,
                                                       "v > 1 AND v < 10 AND m CONTAINS 2", STOCK_RANGE_READ);
        for (Map.Entry<String, String> entry : stockHex.entrySet())
        {
            String where = entry.getKey();
            String stock = entry.getValue();
            ReadCommand command = command("SELECT * FROM " + table + " WHERE " + where + " ALLOW FILTERING", ConsistencyLevel.QUORUM, NOW_IN_SEC);

            for (int version : new int[]{ MessagingService.VERSION_40, MessagingService.VERSION_50 })
            {
                // Control. A command from an Apache Cassandra coordinator keeps this replica strict
                ReadCommand fromStock = deserialize(stock, version);
                assertEquals(where + " at version " + version, stock, serialize(fromStock, version));
                assertEquals(where + " at version " + version, ImmutableSet.of(), kept(fromStock, 0));

                // The flag byte holds 0x80 as well and every other byte matches
                String fork = serialize(command, version);
                assertEquals(where + " at version " + version, stock.substring(0, 2) + "90" + stock.substring(4), fork);
                assertEquals(where + " at version " + version, fork.length() / 2, ReadCommand.serializer.serializedSize(command, version));

                ReadCommand fromFork = deserialize(fork, version);
                assertEquals(where + " at version " + version, fork, serialize(fromFork, version));
                assertEquals(where + " at version " + version, ImmutableSet.of(List.of(1, 2)), kept(fromFork, 0));
            }
        }
    }

    @Test
    public void plainAndStaticAnchorReturnsStaticOnlyPartition() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
        String table = keyspace + '.' + createTable(keyspace, "CREATE TABLE %s (pk int, ck int, s text static, t text static, a text, PRIMARY KEY (pk, ck))");

        // Static rows only. pk 0 matches s only, pk 1 matches t only and pk 2 matches both.
        execute("INSERT INTO " + table + " (pk, s, t) VALUES (0, '1', '0')");
        execute("INSERT INTO " + table + " (pk, s, t) VALUES (1, '0', '1')");
        execute("INSERT INTO " + table + " (pk, s, t) VALUES (2, '1', '1')");

        // Control. With a regular column restricted the replica checks a, and no partition has a row
        ReadCommand regular = command("SELECT pk FROM " + table + " WHERE s = '1' AND a = '1' ALLOW FILTERING", ConsistencyLevel.QUORUM, FBUtilities.nowInSeconds());
        try (ReadExecutionController controller = regular.executionController();
             UnfilteredPartitionIterator partitions = regular.executeLocally(controller))
        {
            assertFalse(partitions.hasNext());
        }

        // The replica checks s, the static column that comes first in name byte order, and returns pk 0 and
        // pk 2 with their static rows and no rows, so replica filtering protection can read those partitions
        // from the other replicas
        ReadCommand command = command("SELECT pk FROM " + table + " WHERE s = '1' AND t = '1' ALLOW FILTERING", ConsistencyLevel.QUORUM, FBUtilities.nowInSeconds());
        assertNull(command.indexQueryPlan());
        Set<Integer> kept = new HashSet<>();
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    assertFalse(partition.hasNext());
                    assertFalse(partition.staticRow().isEmpty());
                    kept.add(Int32Type.instance.compose(partition.partitionKey().getKey()));
                }
            }
        }
        assertEquals(ImmutableSet.of(0, 2), kept);
    }

    @Test
    public void plainAndLocalQuorumWithOneLocalReplicaHasNoBit() throws Throwable
    {
        // A keyspace with replicas in the remote data center is refused unless the ring knows a node there.
        // The test snitch places 127.0.0.4 in that data center. No read is sent to it.
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        InetAddressAndPort remote = InetAddressAndPort.getByName("127.0.0.4");
        metadata.updateHostId(UUID.randomUUID(), remote);
        metadata.updateNormalToken(Util.token("B"), remote);
        String keyspace = null;
        try
        {
            keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = { 'class' : 'NetworkTopologyStrategy', '" + DATA_CENTER + "' : 1, '" + DATA_CENTER_REMOTE + "' : 2 }");
            String table = keyspace + '.' + createTable(keyspace, "CREATE TABLE %s (pk int, ck int, a text, x text, PRIMARY KEY (pk, ck))");
            String query = "SELECT * FROM " + table + " WHERE x = '1' AND a = '1' ALLOW FILTERING";

            // Control. LOCAL_QUORUM waits for the one local replica, which filters strictly as at ONE
            assertEquals(0x10, flags(command(query, ConsistencyLevel.LOCAL_QUORUM, FBUtilities.nowInSeconds())));

            // QUORUM waits for two of the three replicas
            assertEquals(0x90, flags(command(query, ConsistencyLevel.QUORUM, FBUtilities.nowInSeconds())));
        }
        finally
        {
            if (keyspace != null)
                schemaChange("DROP KEYSPACE " + keyspace);
            metadata.removeEndpoint(remote);
        }
    }

    /**
     * @return the read command of the query, the first one for a partition key IN
     */
    private static ReadCommand command(String query, ConsistencyLevel consistency, long nowInSec)
    {
        SelectStatement select = (SelectStatement) QueryProcessor.getStatement(query, ClientState.forInternalCalls());
        ReadQuery read = select.getQuery(QueryOptions.forInternalCalls(consistency, Collections.emptyList()), nowInSec);
        return read instanceof SinglePartitionReadCommand.Group ? ((SinglePartitionReadCommand.Group) read).queries.get(0) : (ReadCommand) read;
    }

    /**
     * @return the flag byte of the command as the coordinator sends it
     */
    private static int flags(ReadCommand command) throws IOException
    {
        return ByteBufferUtil.hexToBytes(serialize(command, MessagingService.current_version)).get(1) & 0xff;
    }

    private static String serialize(ReadCommand command, int version) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ReadCommand.serializer.serialize(command, out, version);
            return ByteBufferUtil.bytesToHex(out.asNewBuffer());
        }
    }

    private static ReadCommand deserialize(String hex, int version) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(ByteBufferUtil.hexToBytes(hex), false))
        {
            ReadCommand command = ReadCommand.serializer.deserialize(in, version);
            assertEquals(0, in.available());
            return command;
        }
    }

    /**
     * @return the partition key and one clustering column of every row the replica keeps
     */
    private static Set<List<Integer>> kept(ReadCommand command, int clusteringColumn)
    {
        Set<List<Integer>> kept = new HashSet<>();
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    int pk = Int32Type.instance.compose(partition.partitionKey().getKey());
                    while (partition.hasNext())
                        kept.add(List.of(pk, Int32Type.instance.compose(partition.next().clustering().bufferAt(clusteringColumn))));
                }
            }
        }
        return kept;
    }
}
