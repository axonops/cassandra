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
package org.apache.cassandra.index.sai.plan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.management.ObjectName;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * How several relations on one column become index expressions.
 */
public class AnalyzedOperationTest extends SAITester
{
    @Test
    public void analyzedRelationsNeverFoldWithoutAnIndex()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, m map<text, text>, t text)");
        createIndex("CREATE INDEX ON %s(VALUES(m)) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        ReadCommand command = PartitionRangeReadCommand.allDataRead(cfs.metadata(), FBUtilities.nowInSeconds());
        QueryController controller = new QueryController(cfs, command, null, new QueryContext(command, DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS)));
        ColumnMetadata m = cfs.metadata().getColumn(ColumnIdentifier.getInterned("m", false));
        ColumnMetadata t = cfs.metadata().getColumn(ColumnIdentifier.getInterned("t", false));

        // MATCH on the values has an index, MATCH KEY on the keys has none. The relation without an index
        // gets an expression of its own and leaves the bound of the indexed one alone.
        RowFilter onMap = RowFilter.create(false);
        onMap.add(m, Operator.ANALYZER_MATCHES, UTF8Type.instance.decompose("alpha"));
        onMap.add(m, Operator.ANALYZER_MATCHES_KEY, UTF8Type.instance.decompose("beta"));
        List<Expression> mapExpressions = Operation.buildIndexExpressions(controller, new ArrayList<>(onMap.getExpressions())).expressionsFor(m);
        assertEquals(mapExpressions.toString(), 2, mapExpressions.size());
        assertFalse(mapExpressions.get(0).isNotIndexed());
        assertEquals("alpha", UTF8Type.instance.compose(mapExpressions.get(0).lower().value.raw));
        assertTrue(mapExpressions.get(1).isNotIndexed());
        assertEquals("beta", UTF8Type.instance.compose(mapExpressions.get(1).lower().value.raw));

        // Two relations with no index keep one expression each, neither is lost
        RowFilter onText = RowFilter.create(false);
        onText.add(t, Operator.ANALYZER_MATCHES, UTF8Type.instance.decompose("x"));
        onText.add(t, Operator.ANALYZER_MATCHES, UTF8Type.instance.decompose("y"));
        List<Expression> textExpressions = Operation.buildIndexExpressions(controller, new ArrayList<>(onText.getExpressions())).expressionsFor(t);
        assertEquals(textExpressions.toString(), 2, textExpressions.size());
        assertTrue(textExpressions.get(0).isNotIndexed());
        assertEquals("x", UTF8Type.instance.compose(textExpressions.get(0).lower().value.raw));
        assertTrue(textExpressions.get(1).isNotIndexed());
        assertEquals("y", UTF8Type.instance.compose(textExpressions.get(1).lower().value.raw));
    }

    @Test
    public void sameColumnEqualityNeverJoinsARange()
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, u int, c text, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck DESC)");
        createIndex("CREATE INDEX ON %s(ck) USING 'sai'");
        createIndex("CREATE INDEX ON %s(a) USING 'sai'");
        createIndex("CREATE INDEX ON %s(c) USING 'sai' WITH OPTIONS = { 'case_sensitive' : false }");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        ReadCommand command = PartitionRangeReadCommand.allDataRead(cfs.metadata(), FBUtilities.nowInSeconds());
        QueryController controller = new QueryController(cfs, command, null, new QueryContext(command, DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS)));
        ColumnMetadata ck = cfs.metadata().getColumn(ColumnIdentifier.getInterned("ck", false));
        ColumnMetadata a = cfs.metadata().getColumn(ColumnIdentifier.getInterned("a", false));
        ColumnMetadata u = cfs.metadata().getColumn(ColumnIdentifier.getInterned("u", false));
        ColumnMetadata c = cfs.metadata().getColumn(ColumnIdentifier.getInterned("c", false));

        // An AND group under an OR holding = and a range on one column, as a coordinator that does not
        // merge the relations of an OR branch sends it. The two expressions stay apart and their
        // intersection is the AND the query means.
        RowFilter equalityAndRange = RowFilter.create(false);
        RowFilter.FilterElement equalityAndRangeGroup = equalityAndRange.root().addOrChild().addAndChild();
        equalityAndRangeGroup.add(a, Operator.EQ, Int32Type.instance.decompose(5));
        equalityAndRangeGroup.add(a, Operator.GT, Int32Type.instance.decompose(0));
        List<Expression> onIndexed = groupUnderOr(controller, equalityAndRange).expressionsFor(a);
        assertEquals(onIndexed.toString(), 2, onIndexed.size());
        assertEquals(Expression.IndexOperator.EQ, onIndexed.get(0).getIndexOperator());
        assertEquals(5, (int) Int32Type.instance.compose(onIndexed.get(0).lower().value.raw));
        assertEquals(Expression.IndexOperator.RANGE, onIndexed.get(1).getIndexOperator());
        assertEquals(0, (int) Int32Type.instance.compose(onIndexed.get(1).lower().value.raw));
        assertNull(onIndexed.get(1).upper());

        // Two = on one column
        RowFilter twoEqualities = RowFilter.create(false);
        RowFilter.FilterElement twoEqualitiesGroup = twoEqualities.root().addOrChild().addAndChild();
        twoEqualitiesGroup.add(a, Operator.EQ, Int32Type.instance.decompose(5));
        twoEqualitiesGroup.add(a, Operator.EQ, Int32Type.instance.decompose(6));
        List<Expression> equalities = groupUnderOr(controller, twoEqualities).expressionsFor(a);
        assertEquals(equalities.toString(), 2, equalities.size());
        assertEquals(5, (int) Int32Type.instance.compose(equalities.get(0).lower().value.raw));
        assertEquals(6, (int) Int32Type.instance.compose(equalities.get(1).lower().value.raw));

        // Two lower bounds on one column
        RowFilter twoLowerBounds = RowFilter.create(false);
        RowFilter.FilterElement twoLowerBoundsGroup = twoLowerBounds.root().addOrChild().addAndChild();
        twoLowerBoundsGroup.add(a, Operator.GT, Int32Type.instance.decompose(2));
        twoLowerBoundsGroup.add(a, Operator.GT, Int32Type.instance.decompose(0));
        List<Expression> lowerBounds = groupUnderOr(controller, twoLowerBounds).expressionsFor(a);
        assertEquals(lowerBounds.toString(), 2, lowerBounds.size());
        assertEquals(2, (int) Int32Type.instance.compose(lowerBounds.get(0).lower().value.raw));
        assertEquals(0, (int) Int32Type.instance.compose(lowerBounds.get(1).lower().value.raw));

        // The same on a column with no index and on a column with a case insensitive index
        RowFilter onUnindexed = RowFilter.create(false);
        RowFilter.FilterElement onUnindexedGroup = onUnindexed.root().addOrChild().addAndChild();
        onUnindexedGroup.add(u, Operator.EQ, Int32Type.instance.decompose(5));
        onUnindexedGroup.add(u, Operator.GT, Int32Type.instance.decompose(0));
        List<Expression> unindexed = groupUnderOr(controller, onUnindexed).expressionsFor(u);
        assertEquals(unindexed.toString(), 2, unindexed.size());
        assertTrue(unindexed.get(0).isNotIndexed());
        assertEquals(Expression.IndexOperator.EQ, unindexed.get(0).getIndexOperator());
        assertEquals(Expression.IndexOperator.RANGE, unindexed.get(1).getIndexOperator());

        RowFilter onCaseInsensitive = RowFilter.create(false);
        RowFilter.FilterElement onCaseInsensitiveGroup = onCaseInsensitive.root().addOrChild().addAndChild();
        onCaseInsensitiveGroup.add(c, Operator.EQ, UTF8Type.instance.decompose("x"));
        onCaseInsensitiveGroup.add(c, Operator.GT, UTF8Type.instance.decompose("a"));
        List<Expression> caseInsensitive = groupUnderOr(controller, onCaseInsensitive).expressionsFor(c);
        assertEquals(caseInsensitive.toString(), 2, caseInsensitive.size());
        assertEquals(Expression.IndexOperator.EQ, caseInsensitive.get(0).getIndexOperator());
        assertEquals("x", UTF8Type.instance.compose(caseInsensitive.get(0).lower().value.raw));
        assertEquals(Expression.IndexOperator.RANGE, caseInsensitive.get(1).getIndexOperator());

        // Guard. A lower and an upper bound share one expression, also on a column in descending order
        RowFilter bothBounds = RowFilter.create(false);
        RowFilter.FilterElement bothBoundsGroup = bothBounds.root().addOrChild().addAndChild();
        bothBoundsGroup.add(a, Operator.GT, Int32Type.instance.decompose(0));
        bothBoundsGroup.add(a, Operator.LT, Int32Type.instance.decompose(3));
        List<Expression> slice = groupUnderOr(controller, bothBounds).expressionsFor(a);
        assertEquals(slice.toString(), 1, slice.size());
        assertEquals(0, (int) Int32Type.instance.compose(slice.get(0).lower().value.raw));
        assertEquals(3, (int) Int32Type.instance.compose(slice.get(0).upper().value.raw));

        RowFilter bothBoundsDescending = RowFilter.create(false);
        RowFilter.FilterElement bothBoundsDescendingGroup = bothBoundsDescending.root().addOrChild().addAndChild();
        bothBoundsDescendingGroup.add(ck, Operator.GT, Int32Type.instance.decompose(0));
        bothBoundsDescendingGroup.add(ck, Operator.LT, Int32Type.instance.decompose(3));
        List<Expression> descending = groupUnderOr(controller, bothBoundsDescending).expressionsFor(ck);
        assertEquals(descending.toString(), 1, descending.size());
        assertNotNull(descending.get(0).lower());
        assertNotNull(descending.get(0).upper());
    }

    @Test
    public void sameColumnPairOutsideOrKeepsTheStockFold()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, a int, height int)");
        createIndex("CREATE INDEX ON %s(a) USING 'sai'");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        ReadCommand command = PartitionRangeReadCommand.allDataRead(cfs.metadata(), FBUtilities.nowInSeconds());
        QueryController controller = new QueryController(cfs, command, null, new QueryContext(command, DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS)));
        ColumnMetadata height = cfs.metadata().getColumn(ColumnIdentifier.getInterned("height", false));

        // Guard. Outside an OR, height > 0 AND height = 5 folds into one range as stock Cassandra folds it,
        // both through the stock method and through an AND node at the root of a tree
        RowFilter pair = RowFilter.create(false);
        pair.add(height, Operator.GT, Int32Type.instance.decompose(0));
        pair.add(height, Operator.EQ, Int32Type.instance.decompose(5));

        List<Expression> direct = Operation.buildIndexExpressions(controller, new ArrayList<>(pair.getExpressions())).expressionsFor(height);
        assertEquals(direct.toString(), 1, direct.size());
        assertEquals(Expression.IndexOperator.RANGE, direct.get(0).getIndexOperator());
        assertEquals(0, (int) Int32Type.instance.compose(direct.get(0).lower().value.raw));
        assertEquals(5, (int) Int32Type.instance.compose(direct.get(0).upper().value.raw));

        List<Expression> atRoot = Operation.Node.buildTree(pair).analyzeTree(controller).expressions.expressionsFor(height);
        assertEquals(atRoot.toString(), 1, atRoot.size());
        assertEquals(Expression.IndexOperator.RANGE, atRoot.get(0).getIndexOperator());
        assertEquals(0, (int) Int32Type.instance.compose(atRoot.get(0).lower().value.raw));
        assertEquals(5, (int) Int32Type.instance.compose(atRoot.get(0).upper().value.raw));
    }

    @Test
    public void replicaKeepsAnUnmergedSameColumnPairApart() throws Throwable
    {
        requireNetwork();
        startJMXServer();
        createMBeanServerConnection();

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, a int, b int)");
        createIndex("CREATE INDEX ON %s(a) USING 'sai'");
        createIndex("CREATE INDEX ON %s(b) USING 'sai'");

        for (int i = 1; i <= 6; i++)
            execute("INSERT INTO %s (pk, a, b) VALUES (?, ?, ?)", i, i, 10 * i);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        ColumnMetadata a = cfs.metadata().getColumn(ColumnIdentifier.getInterned("a", false));
        ColumnMetadata b = cfs.metadata().getColumn(ColumnIdentifier.getInterned("b", false));

        // (a = 5 AND a > 0) OR b = 20 as a coordinator that does not merge the relations of an OR branch
        // sends it to this replica
        RowFilter unmerged = RowFilter.create(false);
        RowFilter.FilterElement or = unmerged.root().addOrChild();
        RowFilter.FilterElement branch = or.addAndChild();
        branch.add(a, Operator.EQ, Int32Type.instance.decompose(5));
        branch.add(a, Operator.GT, Int32Type.instance.decompose(0));
        or.add(b, Operator.EQ, Int32Type.instance.decompose(20));
        ReadCommand fromCoordinator = PartitionRangeReadCommand.create(cfs.metadata(), FBUtilities.nowInSeconds(), ColumnFilter.all(cfs.metadata()),
                                                                       unmerged, DataLimits.NONE, DataRange.allData(cfs.getPartitioner()));

        // Control. The same pair at the root, outside any OR, keeps the stock fold and keeps nothing apart
        RowFilter rootPair = RowFilter.create(false);
        rootPair.add(a, Operator.EQ, Int32Type.instance.decompose(5));
        rootPair.add(a, Operator.GT, Int32Type.instance.decompose(0));
        ReadCommand atRoot = PartitionRangeReadCommand.create(cfs.metadata(), FBUtilities.nowInSeconds(), ColumnFilter.all(cfs.metadata()),
                                                              rootPair, DataLimits.NONE, DataRange.allData(cfs.getPartitioner()));

        // Control. Through CQL the slice pair merges in its branch, and a query without OR has one
        // expression per column, so neither keeps an expression apart.
        ReadCommand merged = (ReadCommand) ((SelectStatement) QueryProcessor.getStatement(formatQuery("SELECT pk FROM %s WHERE (a > 0 AND a < 3) OR b = 50"), ClientState.forInternalCalls()))
                                           .getQuery(QueryOptions.DEFAULT, FBUtilities.nowInSeconds());
        ReadCommand withoutOr = (ReadCommand) ((SelectStatement) QueryProcessor.getStatement(formatQuery("SELECT pk FROM %s WHERE a > 1 AND a < 4"), ClientState.forInternalCalls()))
                                              .getQuery(QueryOptions.DEFAULT, FBUtilities.nowInSeconds());

        ObjectName keptApart = objectNameNoIndex("TotalSameColumnExpressionsKeptApart", KEYSPACE, currentTable(), TableQueryMetrics.TABLE_QUERY_METRIC_TYPE);
        for (ReadCommand command : new ReadCommand[]{ fromCoordinator, merged, withoutOr, atRoot })
        {
            Set<Integer> kept = new HashSet<>();
            TimeUUID session = Tracing.instance.newSession(Tracing.TraceType.QUERY);
            try (ReadExecutionController controller = command.executionController();
                 UnfilteredPartitionIterator partitions = command.executeLocally(controller))
            {
                while (partitions.hasNext())
                {
                    try (UnfilteredRowIterator partition = partitions.next())
                    {
                        if (partition.hasNext())
                            kept.add(Int32Type.instance.compose(partition.partitionKey().getKey()));
                    }
                }
            }
            finally
            {
                Tracing.instance.stopSession();
            }
            waitForTracingEvents();

            List<String> keptApartEvents = new ArrayList<>();
            for (UntypedResultSet.Row event : execute("SELECT activity FROM system_traces.events WHERE session_id = " + session))
            {
                String activity = event.getString("activity");
                if (activity.contains("apart instead of folding them"))
                    keptApartEvents.add(activity);
            }

            if (command == fromCoordinator)
            {
                // The replica returns the exact AND of the branch, the trace names the column and the
                // metric counts the one extra expression
                assertEquals(ImmutableSet.of(2, 5), kept);
                assertEquals(List.of("Index query kept 2 expressions on column a apart instead of folding them"), keptApartEvents);
            }
            else
            {
                // The rows of atRoot are the stock fold of = and a range, which
                // sameColumnPairOutsideOrKeepsTheStockFold pins, so only its trace and metric are checked
                if (command != atRoot)
                    assertEquals(command == merged ? ImmutableSet.of(1, 2, 5) : ImmutableSet.of(2, 3), kept);
                assertEquals(List.of(), keptApartEvents);
            }
            assertEquals(1L, getMetricValue(keptApart));
        }
    }

    /**
     * @return the expressions of the AND group under the OR at the root of the analyzed filter tree
     */
    private static Operation.Expressions groupUnderOr(QueryController controller, RowFilter filter)
    {
        return Operation.Node.buildTree(filter).analyzeTree(controller).children().get(0).children().get(0).expressions;
    }
}
