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

package org.apache.cassandra.db.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RowFilterTest
{

    @Test
    public void testCQLFilterClose()
    {
        // CASSANDRA-15126
        TableMetadata metadata = TableMetadata.builder("testks", "testcf")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addStaticColumn("s", Int32Type.instance)
                                              .addRegularColumn("r", Int32Type.instance)
                                              .build();
        ColumnMetadata s = metadata.getColumn(new ColumnIdentifier("s", true));
        ColumnMetadata r = metadata.getColumn(new ColumnIdentifier("r", true));

        ByteBuffer one = Int32Type.instance.decompose(1);
        RowFilter filter = RowFilter.none().withNewExpressions(new ArrayList<>());
        filter.add(s, Operator.NEQ, one);
        AtomicBoolean closed = new AtomicBoolean();
        UnfilteredPartitionIterator iter = filter.filter(new SingletonUnfilteredPartitionIterator(new UnfilteredRowIterator()
        {
            public DeletionTime partitionLevelDeletion() { return null; }
            public EncodingStats stats() { return null; }
            public TableMetadata metadata() { return metadata; }
            public boolean isReverseOrder() { return false; }
            public RegularAndStaticColumns columns() { return null; }
            public DecoratedKey partitionKey() { return null; }
            public boolean hasNext() { return false; }
            public Unfiltered next() { return null; }
            public Row staticRow()
            {
                return BTreeRow.create(Clustering.STATIC_CLUSTERING,
                                       LivenessInfo.EMPTY,
                                       Row.Deletion.LIVE,
                                       BTree.singleton(new BufferCell(s, 1, Cell.NO_TTL, Cell.NO_DELETION_TIME, one, null)));
            }
            public void close()
            {
                closed.set(true);
            }
        }), 1);
        assertFalse(iter.hasNext());
        assertTrue(closed.get());

        filter = RowFilter.none().withNewExpressions(new ArrayList<>());
        filter.add(r, Operator.NEQ, one);
        closed.set(false);
        iter = filter.filter(new SingletonUnfilteredPartitionIterator(new UnfilteredRowIterator()
        {
            boolean hasNext = true;
            public DeletionTime partitionLevelDeletion() { return null; }
            public EncodingStats stats() { return null; }
            public TableMetadata metadata() { return metadata; }
            public boolean isReverseOrder() { return false; }
            public RegularAndStaticColumns columns() { return null; }
            public DecoratedKey partitionKey() { return null; }
            public Row staticRow() { return Rows.EMPTY_STATIC_ROW; }
            public boolean hasNext()
            {
                boolean r = hasNext;
                hasNext = false;
                return r;
            }
            public Unfiltered next()
            {
                return BTreeRow.create(Clustering.EMPTY,
                                       LivenessInfo.EMPTY,
                                       Row.Deletion.LIVE,
                                       BTree.singleton(new BufferCell(r, 1, Cell.NO_TTL, Cell.NO_DELETION_TIME, one, null)));
            }
            public void close()
            {
                closed.set(true);
            }
        }), 1);
        assertFalse(iter.hasNext());
        assertTrue(closed.get());
    }

    @Test
    public void testMutableIntersections()
    {
        TableMetadata metadata = TableMetadata.builder("testks", "testcf")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addRegularColumn("r", Int32Type.instance)
                                              .addRegularColumn("t", UTF8Type.instance)
                                              .build();

        RowFilter filter = RowFilter.none().withNewExpressions(new ArrayList<>());
        assertFalse(filter.isMutableIntersection());
        
        ColumnMetadata r = metadata.getColumn(new ColumnIdentifier("r", true));
        RowFilter.Expression gt = new RowFilter.SimpleExpression(r, Operator.GT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        filter = filter.withNewExpressions(Collections.singletonList(gt));
        assertFalse(filter.isMutableIntersection());

        RowFilter.Expression lt = new RowFilter.SimpleExpression(r, Operator.LT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        filter = filter.withNewExpressions(ImmutableList.of(gt, lt));
        assertFalse(filter.isMutableIntersection());

        ColumnMetadata t = metadata.getColumn(new ColumnIdentifier("t", true));
        RowFilter.Expression eq = new RowFilter.SimpleExpression(t, Operator.EQ, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        filter = filter.withNewExpressions(ImmutableList.of(gt, lt, eq));
        assertTrue(filter.isMutableIntersection());
    }

    private static TableMetadata treeMetadata()
    {
        return TableMetadata.builder("testks", "testcf")
                            .addPartitionKeyColumn("pk", Int32Type.instance)
                            .addStaticColumn("s", Int32Type.instance)
                            .addRegularColumn("a", Int32Type.instance)
                            .addRegularColumn("b", Int32Type.instance)
                            .addRegularColumn("c", Int32Type.instance)
                            .build();
    }

    private static ColumnMetadata column(TableMetadata metadata, String name)
    {
        return metadata.getColumn(new ColumnIdentifier(name, true));
    }

    private static Row row(TableMetadata metadata, Clustering<?> clustering, Object... columnsAndValues)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(clustering);
        for (int i = 0; i < columnsAndValues.length; i += 2)
        {
            ColumnMetadata def = column(metadata, (String) columnsAndValues[i]);
            builder.addCell(BufferCell.live(def, 1, Int32Type.instance.decompose((Integer) columnsAndValues[i + 1])));
        }
        return builder.build();
    }

    /**
     * Builds a filter for (a = 1 AND b = 2) OR c = 3, a depth two tree.
     */
    private static RowFilter andUnderOrFilter(TableMetadata metadata, boolean needsReconciliation)
    {
        ByteBuffer one = Int32Type.instance.decompose(1);
        ByteBuffer two = Int32Type.instance.decompose(2);
        ByteBuffer three = Int32Type.instance.decompose(3);

        RowFilter filter = RowFilter.create(needsReconciliation);
        RowFilter.FilterElement or = filter.root().addOrChild();
        RowFilter.FilterElement branch = or.addAndChild();
        branch.add(column(metadata, "a"), Operator.EQ, one);
        branch.add(column(metadata, "b"), Operator.EQ, two);
        or.add(column(metadata, "c"), Operator.EQ, three);
        return filter;
    }

    @Test
    public void testTreeEvaluation()
    {
        TableMetadata metadata = treeMetadata();
        RowFilter filter = andUnderOrFilter(metadata, false);

        assertTrue(filter.containsDisjunction());
        assertEquals(3, filter.getExpressions().size());

        // The AND branch requires both of its leaves
        assertTrue(filter.isSatisfiedBy(metadata, null, row(metadata, Clustering.EMPTY, "a", 1, "b", 2), 1));
        assertFalse(filter.isSatisfiedBy(metadata, null, row(metadata, Clustering.EMPTY, "a", 1), 1));
        assertFalse(filter.isSatisfiedBy(metadata, null, row(metadata, Clustering.EMPTY, "b", 2), 1));

        // The OR leaf matches on its own
        assertTrue(filter.isSatisfiedBy(metadata, null, row(metadata, Clustering.EMPTY, "c", 3), 1));
        assertTrue(filter.isSatisfiedBy(metadata, null, row(metadata, Clustering.EMPTY, "a", 1, "c", 3), 1));
        assertFalse(filter.isSatisfiedBy(metadata, null, row(metadata, Clustering.EMPTY, "a", 2, "b", 2, "c", 4), 1));

        // A top level conjunct alongside the disjunction must still match
        ByteBuffer one = Int32Type.instance.decompose(1);
        RowFilter withRootLeaf = andUnderOrFilter(metadata, false);
        withRootLeaf.add(column(metadata, "a"), Operator.EQ, one);
        assertTrue(withRootLeaf.isSatisfiedBy(metadata, null, row(metadata, Clustering.EMPTY, "a", 1, "c", 3), 1));
        assertFalse(withRootLeaf.isSatisfiedBy(metadata, null, row(metadata, Clustering.EMPTY, "c", 3), 1));
    }

    @Test
    public void testStaticLeafUnderDisjunction()
    {
        TableMetadata metadata = treeMetadata();
        ByteBuffer one = Int32Type.instance.decompose(1);
        ByteBuffer two = Int32Type.instance.decompose(2);

        RowFilter filter = RowFilter.create(false);
        RowFilter.FilterElement or = filter.root().addOrChild();
        or.add(column(metadata, "s"), Operator.EQ, one);
        or.add(column(metadata, "a"), Operator.EQ, two);

        Row staticRow = row(metadata, Clustering.STATIC_CLUSTERING, "s", 1);
        Row regularRow = row(metadata, Clustering.EMPTY, "a", 3);

        // The static leaf is evaluated against the static row, not the regular row
        assertTrue(filter.root().isSatisfiedBy(metadata, null, regularRow, staticRow, 1));
        assertFalse(filter.root().isSatisfiedBy(metadata, null, regularRow, Rows.EMPTY_STATIC_ROW, 1));
        assertTrue(filter.root().isSatisfiedBy(metadata, null, row(metadata, Clustering.EMPTY, "a", 2), Rows.EMPTY_STATIC_ROW, 1));
    }

    @Test
    public void testPerNodeMutableIntersections()
    {
        TableMetadata metadata = treeMetadata();
        ByteBuffer one = Int32Type.instance.decompose(1);
        ByteBuffer two = Int32Type.instance.decompose(2);

        // A pure disjunction is never a mutable intersection
        RowFilter orOnly = RowFilter.create(true);
        RowFilter.FilterElement or = orOnly.root().addOrChild();
        or.add(column(metadata, "a"), Operator.EQ, one);
        or.add(column(metadata, "b"), Operator.EQ, two);
        assertFalse(or.isMutableIntersection());
        assertFalse(orOnly.isMutableIntersection());
        assertTrue(orOnly.isStrict());

        // An AND branch under an OR spanning two mutable columns needs the downgrade
        RowFilter branched = andUnderOrFilter(metadata, true);
        RowFilter.FilterElement orNode = branched.root().children().get(0);
        RowFilter.FilterElement andBranch = orNode.children().get(0);
        assertTrue(andBranch.isMutableIntersection());
        assertFalse(orNode.isMutableIntersection());
        assertFalse(branched.root().isMutableIntersection());
        assertTrue(branched.isMutableIntersection());
        assertFalse(branched.isStrict());

        // Without reconciliation the same tree is strict
        assertTrue(andUnderOrFilter(metadata, false).isStrict());

        // A root conjunct plus a disjunction spanning other mutable columns is an intersection
        RowFilter rootAndOr = RowFilter.create(true);
        rootAndOr.add(column(metadata, "c"), Operator.EQ, one);
        RowFilter.FilterElement orChild = rootAndOr.root().addOrChild();
        orChild.add(column(metadata, "a"), Operator.EQ, one);
        orChild.add(column(metadata, "b"), Operator.EQ, two);
        assertTrue(rootAndOr.root().isMutableIntersection());
        assertFalse(rootAndOr.isStrict());

        // A single column AND branch stays strict
        RowFilter sameColumn = RowFilter.create(true);
        RowFilter.FilterElement orNode2 = sameColumn.root().addOrChild();
        RowFilter.FilterElement sameColumnBranch = orNode2.addAndChild();
        sameColumnBranch.add(column(metadata, "a"), Operator.GT, one);
        sameColumnBranch.add(column(metadata, "a"), Operator.LT, two);
        orNode2.add(column(metadata, "b"), Operator.EQ, two);
        assertFalse(sameColumnBranch.isMutableIntersection());
        assertFalse(sameColumn.isMutableIntersection());
        assertTrue(sameColumn.isStrict());

        // A static column intersected with anything is mutable
        RowFilter withStatic = RowFilter.create(true);
        RowFilter.FilterElement orNode3 = withStatic.root().addOrChild();
        RowFilter.FilterElement staticBranch = orNode3.addAndChild();
        staticBranch.add(column(metadata, "s"), Operator.EQ, one);
        staticBranch.add(column(metadata, "a"), Operator.EQ, two);
        orNode3.add(column(metadata, "b"), Operator.EQ, two);
        assertTrue(staticBranch.isMutableIntersection());
        assertTrue(withStatic.isMutableIntersection());
        assertFalse(withStatic.isStrict());
    }

    @Test
    public void testWithoutOnRootWithOnlyDisjunctions()
    {
        TableMetadata metadata = treeMetadata();
        ByteBuffer one = Int32Type.instance.decompose(1);
        ByteBuffer two = Int32Type.instance.decompose(2);

        RowFilter filter = RowFilter.create(false);
        RowFilter.FilterElement or = filter.root().addOrChild();
        or.add(column(metadata, "a"), Operator.EQ, one);
        or.add(column(metadata, "b"), Operator.EQ, two);

        // A query like pk = 1 AND ck = 5 AND (a = 1 OR b = 2) produces a filter whose root has
        // no expressions of its own, and the CQL rendering path removes clustering expressions
        // from such a filter
        RowFilter without = filter.without(column(metadata, "c"), Operator.EQ, one);
        assertTrue(without.containsDisjunction());
        assertEquals(2, without.getExpressions().size());
        assertFalse(without.isEmpty());
    }

    @Test
    public void testTreeSerializationRoundTrip() throws Exception
    {
        TableMetadata metadata = treeMetadata();
        RowFilter filter = andUnderOrFilter(metadata, false);
        filter.add(column(metadata, "a"), Operator.GT, Int32Type.instance.decompose(0));

        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            RowFilter.serializer.serialize(filter, out, MessagingService.VERSION_AXON_50);
            assertEquals(out.getLength(), RowFilter.serializer.serializedSize(filter, MessagingService.VERSION_AXON_50));

            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                RowFilter deserialized = RowFilter.serializer.deserialize(in, MessagingService.VERSION_AXON_50, metadata, true);
                assertSameTree(filter.root(), deserialized.root());
                assertTrue(deserialized.needsReconciliation());
            }
        }
    }

    @Test
    public void testFlatSerializationRoundTripAtBothVersions() throws Exception
    {
        TableMetadata metadata = treeMetadata();
        RowFilter filter = RowFilter.create(false);
        filter.add(column(metadata, "a"), Operator.EQ, Int32Type.instance.decompose(1));
        filter.add(column(metadata, "b"), Operator.GT, Int32Type.instance.decompose(2));

        for (int version : new int[]{ MessagingService.VERSION_50, MessagingService.VERSION_AXON_50 })
        {
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                RowFilter.serializer.serialize(filter, out, version);
                assertEquals(out.getLength(), RowFilter.serializer.serializedSize(filter, version));

                try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
                {
                    RowFilter deserialized = RowFilter.serializer.deserialize(in, version, metadata, false);
                    assertSameTree(filter.root(), deserialized.root());
                    assertFalse(deserialized.needsReconciliation());
                }
            }
        }
    }

    @Test
    public void testFlatSerializationIsByteIdenticalAtLegacyVersion() throws Exception
    {
        TableMetadata metadata = treeMetadata();
        ColumnMetadata a = column(metadata, "a");
        ByteBuffer one = Int32Type.instance.decompose(1);

        RowFilter filter = RowFilter.create(false);
        filter.add(a, Operator.EQ, one);

        try (DataOutputBuffer actual = new DataOutputBuffer();
             DataOutputBuffer expected = new DataOutputBuffer())
        {
            RowFilter.serializer.serialize(filter, actual, MessagingService.VERSION_50);

            // The legacy flat format, written by hand
            expected.writeBoolean(false);
            expected.writeUnsignedVInt32(1);
            expected.writeByte(0); // Kind.SIMPLE ordinal
            ByteBufferUtil.writeWithShortLength(a.name.bytes, expected);
            Operator.EQ.writeTo(expected);
            ByteBufferUtil.writeWithShortLength(one, expected);

            assertEquals(expected.asNewBuffer(), actual.asNewBuffer());
        }
    }

    @Test
    public void testTreeSerializationAtLegacyVersionThrows() throws Exception
    {
        TableMetadata metadata = treeMetadata();
        RowFilter filter = andUnderOrFilter(metadata, false);

        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            RowFilter.serializer.serialize(filter, out, MessagingService.VERSION_50);
            fail("Serializing a disjunctive filter at a legacy version should throw");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("Cannot serialize a disjunctive row filter"));
        }
    }

    private static void assertSameTree(RowFilter.FilterElement expected, RowFilter.FilterElement actual)
    {
        assertEquals(expected.isDisjunction(), actual.isDisjunction());
        assertEquals(expected.expressions(), actual.expressions());
        assertEquals(expected.children().size(), actual.children().size());
        for (int i = 0; i < expected.children().size(); i++)
            assertSameTree(expected.children().get(i), actual.children().get(i));
    }
}
