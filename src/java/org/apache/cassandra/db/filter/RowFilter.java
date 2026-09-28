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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

import com.google.common.base.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkBindValueSet;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;

/**
 * A filter on which rows a given query should include or exclude.
 * <p>
 * This corresponds to the restrictions on rows that are not handled by the query
 * {@link ClusteringIndexFilter}. Some of the expressions of this filter may
 * be handled by a 2ndary index, and the rest is simply filtered out from the
 * result set (the later can only happen if the query was using ALLOW FILTERING).
 * <p>
 * The filter is a tree of {@link FilterElement} nodes whose root is always a conjunction.
 * Queries without OR produce a flat root with no children, and iterating the filter visits
 * the leaf expressions of the whole tree in document order.
 */
public class RowFilter implements Iterable<RowFilter.Expression>
{
    private static final Logger logger = LoggerFactory.getLogger(RowFilter.class);

    public static final Serializer serializer = new Serializer();
    private static final RowFilter NONE = new RowFilter(new FilterElement(false, Collections.emptyList(), Collections.emptyList()), false);

    private final FilterElement root;

    private final boolean needsReconciliation;

    protected RowFilter(FilterElement root, boolean needsReconciliation)
    {
        this.root = root;
        this.needsReconciliation = needsReconciliation;
    }

    /**
     *
     * @param needsReconciliation whether or not this filter belongs to a read that requires coordinator reconciliation
     *
     * @return a new {@link RowFilter} with an empty {@link Expression} list
     */
    public static RowFilter create(boolean needsReconciliation)
    {
        return new RowFilter(new FilterElement(false, new ArrayList<>(), new ArrayList<>()), needsReconciliation);
    }

    public static RowFilter none()
    {
        return NONE;
    }

    public FilterElement root()
    {
        return root;
    }

    public SimpleExpression add(ColumnMetadata def, Operator op, ByteBuffer value)
    {
        return root.add(def, op, value);
    }

    public void addMapEquality(ColumnMetadata def, ByteBuffer key, Operator op, ByteBuffer value)
    {
        root.add(new MapEqualityExpression(def, key, op, value));
    }

    public void addCustomIndexExpression(TableMetadata metadata, IndexMetadata targetIndex, ByteBuffer value)
    {
        root.add(new CustomExpression(metadata, targetIndex, value));
    }

    /**
     * @return every leaf expression of the tree, in document order
     */
    public List<Expression> getExpressions()
    {
        return root.children().isEmpty() ? root.expressions() : root.leaves();
    }

    /**
     * @return true if any node of the tree is a disjunction
     */
    public boolean containsDisjunction()
    {
        return root.containsDisjunction();
    }

    /**
     * @return true if this filter belongs to a read that requires reconciliation at the coordinator
     * @see StatementRestrictions#getRowFilter(IndexRegistry, QueryOptions)
     */
    public boolean needsReconciliation()
    {
        return needsReconciliation;
    }

    /**
     * If this filter belongs to a read that requires reconciliation at the coordinator, and it contains an intersection
     * on two or more non-key (and therefore mutable) columns, we cannot strictly apply it to local, unrepaired rows.
     * When this occurs, we must downgrade the intersection of expressions to a union and leave the coordinator to
     * filter strictly before sending results to the client.
     *
     * @return true if strict filtering is safe
     *
     * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-19018">CASSANDRA-19018</a>
     */
    public boolean isStrict()
    {
        return !needsReconciliation || !isMutableIntersection();
    }

    /**
     * @return true if any node of this filter contains an intersection on either any static column or two regular
     * mutable columns, and therefore needs the CASSANDRA-19018 downgrade somewhere in the tree
     */
    public boolean isMutableIntersection()
    {
        return root.hasMutableIntersection();
    }

    /**
     * Checks if some of the expressions apply to clustering or regular columns.
     * @return {@code true} if some of the expressions apply to clustering or regular columns, {@code false} otherwise.
     */
    public boolean hasExpressionOnClusteringOrRegularColumns()
    {
        for (Expression expression : getExpressions())
        {
            ColumnMetadata column = expression.column();
            if (column.isClusteringColumn() || column.isRegular())
                return true;
        }
        return false;
    }

    /**
     * Note that the application of this transformation does not yet take {@link #isStrict()} into account. This means
     * that even when strict filtering is not safe, expressions will be applied as intersections rather than unions.
     * The filter will always be evaluated strictly in conjunction with replica filtering protection at the 
     * coordinator, however, even after CASSANDRA-19007 is addressed.
     * 
     * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-190007">CASSANDRA-19007</a>
     */
    protected Transformation<BaseRowIterator<?>> filter(TableMetadata metadata, long nowInSec)
    {
        List<Expression> partitionLevelExpressions = new ArrayList<>();
        List<Expression> rowLevelExpressions = new ArrayList<>();
        for (Expression e: root.expressions())
        {
            if (e.column.isStatic() || e.column.isPartitionKey())
                partitionLevelExpressions.add(e);
            else
                rowLevelExpressions.add(e);
        }

        // A subtree evaluates at the partition level only when every leaf is on a static or partition key
        // column, since a static leaf inside a disjunction must not veto the whole partition on its own.
        List<FilterElement> partitionLevelElements = new ArrayList<>();
        List<FilterElement> rowLevelElements = new ArrayList<>();
        for (FilterElement child : root.children())
        {
            if (child.restrictsOnlyStaticOrPartitionKeyColumns())
                partitionLevelElements.add(child);
            else
                rowLevelElements.add(child);
        }

        final boolean filterNonStaticColumns = !rowLevelExpressions.isEmpty() || !rowLevelElements.isEmpty();

        return new Transformation<>()
        {
            DecoratedKey pk;
            Row staticRow;

            @Override
            protected BaseRowIterator<?> applyToPartition(BaseRowIterator<?> partition)
            {
                pk = partition.partitionKey();
                staticRow = partition.staticRow();

                // Short-circuit all partitions that won't match based on static and partition keys
                for (Expression e : partitionLevelExpressions)
                    if (!e.isSatisfiedBy(metadata, pk, staticRow, nowInSec))
                    {
                        partition.close();
                        return null;
                    }

                for (FilterElement element : partitionLevelElements)
                    if (!element.isSatisfiedBy(metadata, pk, staticRow, staticRow, nowInSec))
                    {
                        partition.close();
                        return null;
                    }

                BaseRowIterator<?> iterator = partition instanceof UnfilteredRowIterator
                                              ? Transformation.apply((UnfilteredRowIterator) partition, this)
                                              : Transformation.apply((RowIterator) partition, this);

                if (filterNonStaticColumns && !iterator.hasNext())
                {
                    iterator.close();
                    return null;
                }

                return iterator;
            }

            @Override
            public Row applyToRow(Row row)
            {
                // If we purge deletions when reconciliation is required, we hide information replica filtering
                // protection would require to filter rows that are no longer matches are the coordinator.
                Row purged = needsReconciliation() ? row : row.purge(DeletionPurger.PURGE_ALL, nowInSec, metadata.enforceStrictLiveness());

                if (purged == null)
                    return null;

                for (Expression e : rowLevelExpressions)
                    if (!e.isSatisfiedBy(metadata, pk, purged, nowInSec))
                        return null;

                for (FilterElement element : rowLevelElements)
                    if (!element.isSatisfiedBy(metadata, pk, purged, staticRow, nowInSec))
                        return null;

                return row;
            }
        };
    }

    /**
     * Filters the provided iterator so that only the row satisfying the expression of this filter
     * are included in the resulting iterator.
     *
     * @param iter the iterator to filter
     * @param nowInSec the time of query in seconds.
     * @return the filtered iterator.
     */
    public UnfilteredPartitionIterator filter(UnfilteredPartitionIterator iter, long nowInSec)
    {
        return isEmpty() ? iter : Transformation.apply(iter, filter(iter.metadata(), nowInSec));
    }

    /**
     * Filters the provided iterator so that only the row satisfying the expression of this filter
     * are included in the resulting iterator.
     *
     * @param iter the iterator to filter
     * @param nowInSec the time of query in seconds.
     * @return the filtered iterator.
     */
    public PartitionIterator filter(PartitionIterator iter, TableMetadata metadata, long nowInSec)
    {
        return isEmpty() ? iter : Transformation.apply(iter, filter(metadata, nowInSec));
    }

    /**
     * Whether the provided row in the provided partition satisfies this filter.
     *
     * @param metadata the table metadata.
     * @param partitionKey the partition key for partition to test.
     * @param row the row to test.
     * @param nowInSec the current time in seconds (to know what is live and what isn't).
     * @return {@code true} if {@code row} in partition {@code partitionKey} satisfies this row filter.
     */
    public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec)
    {
        // We purge all tombstones as the expressions isSatisfiedBy methods expects it
        Row purged = row.purge(DeletionPurger.PURGE_ALL, nowInSec, metadata.enforceStrictLiveness());
        if (purged == null)
            return isEmpty();

        return root.isSatisfiedBy(metadata, partitionKey, purged, purged, nowInSec);
    }

    /**
     * Returns true if all of the expressions within this filter that apply to the partition key are satisfied by
     * the given key, false otherwise.
     */
    public boolean partitionKeyRestrictionsAreSatisfiedBy(DecoratedKey key, AbstractType<?> keyValidator)
    {
        for (Expression e : getExpressions())
        {
            if (!e.column.isPartitionKey())
                continue;

            ByteBuffer value = keyValidator instanceof CompositeType
                             ? ((CompositeType) keyValidator).split(key.getKey())[e.column.position()]
                             : key.getKey();
            if (!e.operator().isSatisfiedBy(e.column.type, value, e.value))
                return false;
        }
        return true;
    }

    /**
     * Returns true if all of the expressions within this filter that apply to the clustering key are satisfied by
     * the given Clustering, false otherwise.
     */
    public boolean clusteringKeyRestrictionsAreSatisfiedBy(Clustering<?> clustering)
    {
        for (Expression e : getExpressions())
        {
            if (!e.column.isClusteringColumn())
                continue;

            if (!e.operator().isSatisfiedBy(e.column.type, clustering.bufferAt(e.column.position()), e.value))
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns this filter but without the provided expression. This method
     * *assumes* that the filter contains the provided expression. Removing a leaf from a
     * disjunction would change semantics, so the expression must live on the root node.
     */
    public RowFilter without(Expression expression)
    {
        assert root.expressions().contains(expression);
        if (root.expressions().size() == 1 && root.children().isEmpty())
            return RowFilter.none();

        List<Expression> newExpressions = new ArrayList<>(Math.max(0, root.expressions().size() - 1));
        for (Expression e : root.expressions())
            if (!e.equals(expression))
                newExpressions.add(e);

        return new RowFilter(new FilterElement(false, newExpressions, root.children()), needsReconciliation);
    }

    /**
     * Returns a copy of this filter but without the provided expression. If this filter doesn't contain the specified
     * expression this method will just return an identical copy of this filter. Only root node expressions are
     * removed, since removing a leaf from a disjunction would change semantics.
     */
    public RowFilter without(ColumnMetadata column, Operator op, ByteBuffer value)
    {
        if (isEmpty())
            return this;

        // The root may hold no expressions of its own when the filter is only a disjunction
        List<Expression> newExpressions = new ArrayList<>(Math.max(0, root.expressions().size() - 1));
        for (Expression e : root.expressions())
            if (!e.column().equals(column) || e.operator() != op || !e.value.equals(value))
                newExpressions.add(e);

        return new RowFilter(new FilterElement(false, newExpressions, root.children()), needsReconciliation);
    }

    public boolean hasNonKeyExpression()
    {
        for (Expression e : getExpressions())
            if (!e.column().isPrimaryKeyColumn())
                return true;

        return false;
    }

    public boolean hasStaticExpression()
    {
        for (Expression e : getExpressions())
            if (e.column().isStatic())
                return true;

        return false;
    }

    public RowFilter withoutExpressions()
    {
        return withNewExpressions(Collections.emptyList());
    }

    /**
     * Returns a copy of this filter with the root expressions only, dropping every child subtree.
     */
    public RowFilter withoutDisjunctions()
    {
        if (root.children().isEmpty())
            return this;

        return new RowFilter(new FilterElement(false, root.expressions(), Collections.emptyList()), needsReconciliation);
    }

    protected RowFilter withNewExpressions(List<Expression> expressions)
    {
        return new RowFilter(new FilterElement(false, expressions, Collections.emptyList()), needsReconciliation);
    }

    public boolean isEmpty()
    {
        return root.isEmpty();
    }

    public Iterator<Expression> iterator()
    {
        return getExpressions().iterator();
    }

    @Override
    public String toString()
    {
        return toString(false);
    }

    /**
     * Returns a CQL representation of this row filter.
     *
     * @return a CQL representation of this row filter
     */
    public String toCQLString()
    {
        return toString(true);
    }

    private String toString(boolean cql)
    {
        return root.toString(cql);
    }

    /**
     * A node of the filter tree. A node is either a conjunction (AND) or a disjunction (OR) over
     * its local leaf expressions and its child nodes. The root of a {@link RowFilter} is always a
     * conjunction, so flat filters are a root with no children.
     */
    public static class FilterElement
    {
        private final boolean isDisjunction;
        private final List<Expression> expressions;
        private final List<FilterElement> children;

        public FilterElement(boolean isDisjunction, List<Expression> expressions, List<FilterElement> children)
        {
            this.isDisjunction = isDisjunction;
            this.expressions = expressions;
            this.children = children;
        }

        public boolean isDisjunction()
        {
            return isDisjunction;
        }

        public List<Expression> expressions()
        {
            return expressions;
        }

        public List<FilterElement> children()
        {
            return children;
        }

        public SimpleExpression add(ColumnMetadata def, Operator op, ByteBuffer value)
        {
            SimpleExpression expression = new SimpleExpression(def, op, value);
            add(expression);
            return expression;
        }

        public void add(Expression expression)
        {
            expression.validate();
            expressions.add(expression);
        }

        public FilterElement addOrChild()
        {
            return newChild(true);
        }

        public FilterElement addAndChild()
        {
            return newChild(false);
        }

        public void addChild(FilterElement child)
        {
            children.add(child);
        }

        private FilterElement newChild(boolean childIsDisjunction)
        {
            FilterElement child = new FilterElement(childIsDisjunction, new ArrayList<>(), new ArrayList<>());
            children.add(child);
            return child;
        }

        public boolean isEmpty()
        {
            if (!expressions.isEmpty())
                return false;

            for (FilterElement child : children)
                if (!child.isEmpty())
                    return false;

            return true;
        }

        /**
         * @return every leaf expression of this node and its descendants, in document order
         */
        public List<Expression> leaves()
        {
            List<Expression> allLeaves = new ArrayList<>(expressions.size());
            gatherLeaves(allLeaves);
            return allLeaves;
        }

        private void gatherLeaves(List<Expression> collector)
        {
            collector.addAll(expressions);
            for (FilterElement child : children)
                child.gatherLeaves(collector);
        }

        public boolean containsDisjunction()
        {
            if (isDisjunction)
                return true;

            for (FilterElement child : children)
                if (child.containsDisjunction())
                    return true;

            return false;
        }

        boolean restrictsOnlyStaticOrPartitionKeyColumns()
        {
            for (Expression e : leaves())
                if (!e.column().isStatic() && !e.column().isPartitionKey())
                    return false;

            return true;
        }

        /**
         * A conjunction node over two or more mutable entities cannot be evaluated strictly against
         * local, possibly partial, rows when the read needs reconciliation, because each replica may
         * hold only some of the intersected cells. Disjunctions only widen results, which is the safe
         * direction, so they never count.
         *
         * @return true if this node intersects either any static column or two distinct mutable
         * columns, counting the columns of its local expressions and all of its descendants
         *
         * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-19018">CASSANDRA-19018</a>
         */
        public boolean isMutableIntersection()
        {
            if (isDisjunction)
                return false;

            // A single conjunct is not an intersection, whatever columns it spans
            if (expressions.size() + children.size() < 2)
                return false;

            Set<ColumnMetadata> columns = null;
            for (Expression e : leaves())
            {
                if (e.column().isStatic())
                    return true;

                if (!e.column().isPrimaryKeyColumn())
                {
                    if (columns == null)
                        columns = new HashSet<>();

                    columns.add(e.column());
                    if (columns.size() > 1)
                        return true;
                }
            }
            return false;
        }

        /**
         * @return true if this node or any of its descendants is a mutable intersection
         */
        public boolean hasMutableIntersection()
        {
            if (isMutableIntersection())
                return true;

            for (FilterElement child : children)
                if (child.hasMutableIntersection())
                    return true;

            return false;
        }

        /**
         * Strict evaluation of this node against the given row. Conjunctions require every local
         * expression and child to match, disjunctions require at least one. Static leaves are
         * evaluated against the given static row so that they work inside disjunctions evaluated
         * per row.
         */
        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row, Row staticRow, long nowInSec)
        {
            for (Expression e : expressions)
            {
                Row localRow = e.column().isStatic() ? staticRow : row;
                if (e.isSatisfiedBy(metadata, partitionKey, localRow, nowInSec) == isDisjunction)
                    return isDisjunction;
            }

            for (FilterElement child : children)
            {
                if (child.isSatisfiedBy(metadata, partitionKey, row, staticRow, nowInSec) == isDisjunction)
                    return isDisjunction;
            }

            return !isDisjunction;
        }

        @Override
        public String toString()
        {
            return toString(false);
        }

        private String toString(boolean cql)
        {
            StringBuilder sb = new StringBuilder();
            String separator = isDisjunction ? " OR " : " AND ";
            for (Expression e : expressions)
            {
                if (sb.length() > 0)
                    sb.append(separator);
                sb.append(e.toString(cql));
            }
            for (FilterElement child : children)
            {
                if (sb.length() > 0)
                    sb.append(separator);
                sb.append('(').append(child.toString(cql)).append(')');
            }
            return sb.toString();
        }
    }

    public static abstract class Expression
    {
        private static final Serializer serializer = new Serializer();

        // Note: the order of this enum matter, it's used for serialization,
        // and this is why we have some UNUSEDX for values we don't use anymore
        // (we could clean those on a major protocol update, but it's not worth
        // the trouble for now)
        protected enum Kind { SIMPLE, MAP_EQUALITY, UNUSED1, CUSTOM, USER }

        protected abstract Kind kind();
        protected final ColumnMetadata column;
        protected final Operator operator;
        protected final ByteBuffer value;

        protected Expression(ColumnMetadata column, Operator operator, ByteBuffer value)
        {
            this.column = column;
            this.operator = operator;
            this.value = value;
        }

        public boolean isCustom()
        {
            return kind() == Kind.CUSTOM;
        }

        public boolean isUserDefined()
        {
            return kind() == Kind.USER;
        }

        public ColumnMetadata column()
        {
            return column;
        }

        public Operator operator()
        {
            return operator;
        }

        /**
         * Checks if the operator of this <code>IndexExpression</code> is a <code>CONTAINS</code> operator.
         *
         * @return <code>true</code> if the operator of this <code>IndexExpression</code> is a <code>CONTAINS</code>
         * operator, <code>false</code> otherwise.
         */
        public boolean isContains()
        {
            return Operator.CONTAINS == operator;
        }

        /**
         * Checks if the operator of this <code>IndexExpression</code> is a <code>CONTAINS_KEY</code> operator.
         *
         * @return <code>true</code> if the operator of this <code>IndexExpression</code> is a <code>CONTAINS_KEY</code>
         * operator, <code>false</code> otherwise.
         */
        public boolean isContainsKey()
        {
            return Operator.CONTAINS_KEY == operator;
        }

        /**
         * If this expression is used to query an index, the value to use as
         * partition key for that index query.
         */
        public ByteBuffer getIndexValue()
        {
            return value;
        }

        public void validate()
        {
            checkNotNull(value, "Unsupported null value for column %s", column.name);
            checkBindValueSet(value, "Unsupported unset value for column %s", column.name);
        }

        /** @deprecated See CASSANDRA-6377 */
        @Deprecated(since = "3.5")
        public void validateForIndexing()
        {
            checkFalse(value.remaining() > FBUtilities.MAX_UNSIGNED_SHORT,
                       "Index expression values may not be larger than 64K");
        }

        /**
         * Returns whether the provided row satisfied this expression or not.
         *
         *
         * @param metadata
         * @param partitionKey the partition key for row to check.
         * @param row the row to check. It should *not* contain deleted cells
         * (i.e. it should come from a RowIterator).
         * @return whether the row is satisfied by this expression.
         */
        public abstract boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec);

        protected ByteBuffer getValue(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec)
        {
            switch (column.kind)
            {
                case PARTITION_KEY:
                    return metadata.partitionKeyType instanceof CompositeType
                         ? CompositeType.extractComponent(partitionKey.getKey(), column.position())
                         : partitionKey.getKey();
                case CLUSTERING:
                    return row.clustering().bufferAt(column.position());
                default:
                    Cell<?> cell = row.getCell(column);
                    return cell == null || cell.isTombstone() || !cell.isLive(nowInSec) ? null : cell.buffer();
            }
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof Expression))
                return false;

            Expression that = (Expression)o;

            return Objects.equal(this.kind(), that.kind())
                && Objects.equal(this.column.name, that.column.name)
                && Objects.equal(this.operator, that.operator)
                && Objects.equal(this.value, that.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column.name, operator, value);
        }

        @Override
        public String toString()
        {
            return toString(false);
        }

        /**
         * Returns a CQL representation of this expression.
         *
         * @return a CQL representation of this expression
         */
        public String toCQLString()
        {
            return toString(true);
        }

        protected abstract String toString(boolean cql);

        private static class Serializer
        {
            public void serialize(Expression expression, DataOutputPlus out, int version) throws IOException
            {
                out.writeByte(expression.kind().ordinal());

                // Custom expressions include neither a column or operator, but all
                // other expressions do.
                if (expression.kind() == Kind.CUSTOM)
                {
                    IndexMetadata.serializer.serialize(((CustomExpression)expression).targetIndex, out, version);
                    ByteBufferUtil.writeWithShortLength(expression.value, out);
                    return;
                }

                if (expression.kind() == Kind.USER)
                {
                    UserExpression.serialize((UserExpression)expression, out, version);
                    return;
                }

                ByteBufferUtil.writeWithShortLength(expression.column.name.bytes, out);
                expression.operator.writeTo(out);

                switch (expression.kind())
                {
                    case SIMPLE:
                        ByteBufferUtil.writeWithShortLength(expression.value, out);
                        break;
                    case MAP_EQUALITY:
                        MapEqualityExpression mexpr = (MapEqualityExpression)expression;
                        ByteBufferUtil.writeWithShortLength(mexpr.key, out);
                        ByteBufferUtil.writeWithShortLength(mexpr.value, out);
                        break;
                }
            }

            public Expression deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
            {
                Kind kind = Kind.values()[in.readByte()];

                // custom expressions (3.0+ only) do not contain a column or operator, only a value
                if (kind == Kind.CUSTOM)
                {
                    return new CustomExpression(metadata,
                            IndexMetadata.serializer.deserialize(in, version, metadata),
                            ByteBufferUtil.readWithShortLength(in));
                }

                if (kind == Kind.USER)
                    return UserExpression.deserialize(in, version, metadata);

                ByteBuffer name = ByteBufferUtil.readWithShortLength(in);
                Operator operator = Operator.readFrom(in);
                ColumnMetadata column = metadata.getColumn(name);

                // Compact storage tables, when used with thrift, used to allow falling through this withouot throwing an
                // exception. However, since thrift was removed in 4.0, this behaviour was not restored in CASSANDRA-16217
                if (column == null)
                    throw new RuntimeException("Unknown (or dropped) column " + UTF8Type.instance.getString(name) + " during deserialization");

                switch (kind)
                {
                    case SIMPLE:
                        return new SimpleExpression(column, operator, ByteBufferUtil.readWithShortLength(in));
                    case MAP_EQUALITY:
                        ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                        ByteBuffer value = ByteBufferUtil.readWithShortLength(in);
                        return new MapEqualityExpression(column, key, operator, value);
                }
                throw new AssertionError();
            }

            public long serializedSize(Expression expression, int version)
            {
                long size = 1; // kind byte

                // Custom expressions include neither a column or operator, but all
                // other expressions do.
                if (expression.kind() != Kind.CUSTOM && expression.kind() != Kind.USER)
                    size += ByteBufferUtil.serializedSizeWithShortLength(expression.column().name.bytes)
                            + expression.operator.serializedSize();

                switch (expression.kind())
                {
                    case SIMPLE:
                        size += ByteBufferUtil.serializedSizeWithShortLength(((SimpleExpression)expression).value);
                        break;
                    case MAP_EQUALITY:
                        MapEqualityExpression mexpr = (MapEqualityExpression)expression;
                        size += ByteBufferUtil.serializedSizeWithShortLength(mexpr.key)
                              + ByteBufferUtil.serializedSizeWithShortLength(mexpr.value);
                        break;
                    case CUSTOM:
                        size += IndexMetadata.serializer.serializedSize(((CustomExpression)expression).targetIndex, version)
                               + ByteBufferUtil.serializedSizeWithShortLength(expression.value);
                        break;
                    case USER:
                        size += UserExpression.serializedSize((UserExpression)expression, version);
                        break;
                }
                return size;
            }
        }
    }

    /**
     * An expression of the form 'column' 'op' 'value'.
     */
    public static class SimpleExpression extends Expression
    {
        // Lazily resolved analysis view of the index backing an analyzed operator, cached across the
        // rows of one query. Always re-derived from schema through the registry, never serialized.
        private Optional<Index.Analyzer> analyzer;

        SimpleExpression(ColumnMetadata column, Operator operator, ByteBuffer value)
        {
            super(column, operator, value);
        }

        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec)
        {
            // We support null conditions for LWT (in ColumnCondition) but not for RowFilter.
            // TODO: we should try to merge both code someday.
            assert value != null;

            switch (operator)
            {
                case EQ:
                case IN:
                case LT:
                case LTE:
                case GTE:
                case GT:
                    {
                        assert !column.isComplex() : "Only CONTAINS and CONTAINS_KEY are supported for collection types";

                        // In order to support operators on Counter types, their value has to be extracted from internal
                        // representation. See CASSANDRA-11629
                        if (column.type.isCounter())
                        {
                            ByteBuffer foundValue = getValue(metadata, partitionKey, row, nowInSec);
                            if (foundValue == null)
                                return false;

                            ByteBuffer counterValue = LongType.instance.decompose(CounterContext.instance().total(foundValue, ByteBufferAccessor.instance));
                            return operator.isSatisfiedBy(LongType.instance, counterValue, value);
                        }
                        else
                        {
                            // Note that CQL expression are always of the form 'x < 4', i.e. the tested value is on the left.
                            ByteBuffer foundValue = getValue(metadata, partitionKey, row, nowInSec);
                            return foundValue != null && operator.isSatisfiedBy(column.type, foundValue, value);
                        }
                    }
                case NEQ:
                case LIKE_PREFIX:
                case LIKE_SUFFIX:
                case LIKE_CONTAINS:
                case LIKE_MATCHES:
                case ANN:
                    {
                        assert !column.isComplex() : "Only CONTAINS and CONTAINS_KEY are supported for collection types";
                        ByteBuffer foundValue = getValue(metadata, partitionKey, row, nowInSec);
                        // Note that CQL expression are always of the form 'x < 4', i.e. the tested value is on the left.
                        return foundValue != null && operator.isSatisfiedBy(column.type, foundValue, value);
                    }
                case ANALYZER_MATCHES:
                case PHRASE:
                    {
                        // Re-analyzes the stored value with the index analyzer, since raw byte
                        // comparison cannot evaluate the analyzed operators. This also runs on the
                        // coordinator during replica filtering protection re-checks, so merged rows
                        // get the same analysis the replicas applied.
                        Index.Analyzer indexAnalyzer = analyzer(metadata);
                        if (indexAnalyzer == null)
                            // The analyzed index was dropped mid-query. Without its analyzer the
                            // operator cannot match anything.
                            return false;

                        if (column.isComplex())
                        {
                            ComplexColumnData complexData = row.getComplexColumnData(column);
                            if (complexData == null)
                                return false;

                            // Each collection element is analyzed on its own, so a phrase never
                            // matches across element boundaries.
                            boolean elementIsCellPath = column.type instanceof SetType;
                            for (Cell<?> cell : complexData)
                            {
                                ByteBuffer element = elementIsCellPath ? cell.path().get(0) : cell.buffer();
                                if (analyzedMatch(indexAnalyzer, element))
                                    return true;
                            }
                            return false;
                        }

                        ByteBuffer foundValue = getValue(metadata, partitionKey, row, nowInSec);
                        return foundValue != null && analyzedMatch(indexAnalyzer, foundValue);
                    }
                case CONTAINS:
                    assert column.type.isCollection();
                    CollectionType<?> type = (CollectionType<?>)column.type;
                    if (column.isComplex())
                    {
                        ComplexColumnData complexData = row.getComplexColumnData(column);
                        if (complexData != null)
                        {
                            for (Cell<?> cell : complexData)
                            {
                                if (type.kind == CollectionType.Kind.SET)
                                {
                                    if (type.nameComparator().compare(cell.path().get(0), value) == 0)
                                        return true;
                                }
                                else
                                {
                                    if (type.valueComparator().compare(cell.buffer(), value) == 0)
                                        return true;
                                }
                            }
                        }
                        return false;
                    }
                    else
                    {
                        ByteBuffer foundValue = getValue(metadata, partitionKey, row, nowInSec);
                        if (foundValue == null)
                            return false;

                        switch (type.kind)
                        {
                            case LIST:
                                ListType<?> listType = (ListType<?>)type;
                                return listType.compose(foundValue).contains(listType.getElementsType().compose(value));
                            case SET:
                                SetType<?> setType = (SetType<?>)type;
                                return setType.compose(foundValue).contains(setType.getElementsType().compose(value));
                            case MAP:
                                MapType<?,?> mapType = (MapType<?, ?>)type;
                                return mapType.compose(foundValue).containsValue(mapType.getValuesType().compose(value));
                        }
                        throw new AssertionError();
                    }
                case CONTAINS_KEY:
                    assert column.type.isCollection() && column.type instanceof MapType;
                    MapType<?, ?> mapType = (MapType<?, ?>)column.type;
                    if (column.isComplex())
                    {
                         return row.getCell(column, CellPath.create(value)) != null;
                    }
                    else
                    {
                        ByteBuffer foundValue = getValue(metadata, partitionKey, row, nowInSec);
                        return foundValue != null && mapType.getSerializer().getSerializedValue(foundValue, value, mapType.getKeysType()) != null;
                    }
            }
            throw new AssertionError();
        }

        private boolean analyzedMatch(Index.Analyzer indexAnalyzer, ByteBuffer storedValue)
        {
            return operator == Operator.PHRASE ? indexAnalyzer.matchesPhrase(storedValue, value)
                                               : indexAnalyzer.matches(storedValue, value);
        }

        @Nullable
        private Index.Analyzer analyzer(TableMetadata metadata)
        {
            // Benign race: concurrent first calls both resolve the same registry entry
            if (analyzer == null)
                analyzer = IndexRegistry.obtain(metadata).analyzerFor(column);
            return analyzer.orElse(null);
        }

        @Override
        protected String toString(boolean cql)
        {
            AbstractType<?> type = column.type;
            switch (operator)
            {
                case CONTAINS:
                    assert type instanceof CollectionType;
                    CollectionType<?> ct = (CollectionType<?>)type;
                    type = ct.kind == CollectionType.Kind.SET ? ct.nameComparator() : ct.valueComparator();
                    break;
                case CONTAINS_KEY:
                    assert type instanceof MapType;
                    type = ((MapType<?, ?>)type).nameComparator();
                    break;
                case IN:
                    type = ListType.getInstance(type, false);
                    break;
                case ANALYZER_MATCHES:
                case PHRASE:
                    // On collections the analyzed operators compare elements, so the value is an element
                    if (type.isCollection() && type.isMultiCell())
                    {
                        CollectionType<?> collection = (CollectionType<?>) type;
                        type = collection.kind == CollectionType.Kind.SET ? collection.nameComparator() : collection.valueComparator();
                    }
                    break;
                default:
                    break;
            }
            return cql
                 ? String.format("%s %s %s", column.name.toCQLString(), operator, type.toCQLString(value) )
                 : String.format("%s %s %s", column.name.toString(), operator, type.getString(value));
        }

        @Override
        protected Kind kind()
        {
            return Kind.SIMPLE;
        }
    }

    /**
     * An expression of the form 'column' ['key'] = 'value' (which is only
     * supported when 'column' is a map).
     */
    private static class MapEqualityExpression extends Expression
    {
        private final ByteBuffer key;

        public MapEqualityExpression(ColumnMetadata column, ByteBuffer key, Operator operator, ByteBuffer value)
        {
            super(column, operator, value);
            assert column.type instanceof MapType && operator == Operator.EQ;
            this.key = key;
        }

        @Override
        public void validate() throws InvalidRequestException
        {
            checkNotNull(key, "Unsupported null map key for column %s", column.name);
            checkBindValueSet(key, "Unsupported unset map key for column %s", column.name);
            checkNotNull(value, "Unsupported null map value for column %s", column.name);
            checkBindValueSet(value, "Unsupported unset map value for column %s", column.name);
        }

        @Override
        public ByteBuffer getIndexValue()
        {
            return CompositeType.build(ByteBufferAccessor.instance, key, value);
        }

        @Override
        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec)
        {
            assert key != null;
            // We support null conditions for LWT (in ColumnCondition) but not for RowFilter.
            // TODO: we should try to merge both code someday.
            assert value != null;

            if (row.isStatic() != column.isStatic())
                return true;

            MapType<?, ?> mt = (MapType<?, ?>)column.type;
            if (column.isComplex())
            {
                Cell<?> cell = row.getCell(column, CellPath.create(key));
                return cell != null && mt.valueComparator().compare(cell.buffer(), value) == 0;
            }
            else
            {
                ByteBuffer serializedMap = getValue(metadata, partitionKey, row, nowInSec);
                if (serializedMap == null)
                    return false;

                ByteBuffer foundValue = mt.getSerializer().getSerializedValue(serializedMap, key, mt.getKeysType());
                return foundValue != null && mt.valueComparator().compare(foundValue, value) == 0;
            }
        }

        @Override
        protected String toString(boolean cql)
        {
            MapType<?, ?> mt = (MapType<?, ?>) column.type;
            AbstractType<?> nt = mt.nameComparator();
            AbstractType<?> vt = mt.valueComparator();
            return cql
                 ? String.format("%s[%s] = %s", column.name.toCQLString(), nt.toCQLString(key), vt.toCQLString(value))
                 : String.format("%s[%s] = %s", column.name.toString(), nt.getString(key), vt.getString(value));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof MapEqualityExpression))
                return false;

            MapEqualityExpression that = (MapEqualityExpression)o;

            return Objects.equal(this.column.name, that.column.name)
                && Objects.equal(this.operator, that.operator)
                && Objects.equal(this.key, that.key)
                && Objects.equal(this.value, that.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column.name, operator, key, value);
        }

        @Override
        protected Kind kind()
        {
            return Kind.MAP_EQUALITY;
        }
    }

    /**
     * A custom index expression for use with 2i implementations which support custom syntax and which are not
     * necessarily linked to a single column in the base table.
     */
    public static final class CustomExpression extends Expression
    {
        private final IndexMetadata targetIndex;
        private final TableMetadata table;

        public CustomExpression(TableMetadata table, IndexMetadata targetIndex, ByteBuffer value)
        {
            // The operator is not relevant, but Expression requires it so for now we just hardcode EQ
            super(makeDefinition(table, targetIndex), Operator.EQ, value);
            this.targetIndex = targetIndex;
            this.table = table;
        }

        private static ColumnMetadata makeDefinition(TableMetadata table, IndexMetadata index)
        {
            // Similarly to how we handle non-defined columns in thift, we create a fake column definition to
            // represent the target index. This is definitely something that can be improved though.
            return ColumnMetadata.regularColumn(table, ByteBuffer.wrap(index.name.getBytes()), BytesType.instance);
        }

        public IndexMetadata getTargetIndex()
        {
            return targetIndex;
        }

        public ByteBuffer getValue()
        {
            return value;
        }

        @Override
        protected String toString(boolean cql)
        {
            return String.format("expr(%s, %s)",
                                 cql ? ColumnIdentifier.maybeQuote(targetIndex.name) : targetIndex.name,
                                 Keyspace.openAndGetStore(table)
                                         .indexManager
                                         .getIndex(targetIndex)
                                         .customExpressionValueType());
        }

        protected Kind kind()
        {
            return Kind.CUSTOM;
        }

        // Filtering by custom expressions isn't supported yet, so just accept any row
        @Override
        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec)
        {
            return true;
        }
    }

    /**
     * A user defined filtering expression. These may be added to RowFilter programmatically by a
     * QueryHandler implementation. No concrete implementations are provided and adding custom impls
     * to the classpath is a task for operators (needless to say, this is something of a power
     * user feature). Care must also be taken to register implementations, via the static register
     * method during system startup. An implementation and its corresponding Deserializer must be
     * registered before sending or receiving any messages containing expressions of that type.
     * Use of custom filtering expressions in a mixed version cluster should be handled with caution
     * as the order in which types are registered is significant: if continuity of use during upgrades
     * is important, new types should registered last and obsoleted types should still be registered (
     * or dummy implementations registered in their place) to preserve consistent identifiers across
     * the cluster).
     *
     * During serialization, the identifier for the Deserializer implementation is prepended to the
     * implementation specific payload. To deserialize, the identifier is read first to obtain the
     * Deserializer, which then provides the concrete expression instance.
     */
    public static abstract class UserExpression extends Expression
    {
        private static final DeserializerRegistry deserializers = new DeserializerRegistry();
        private static final class DeserializerRegistry
        {
            private final AtomicInteger counter = new AtomicInteger(0);
            private final ConcurrentMap<Integer, Deserializer> deserializers = new ConcurrentHashMap<>();
            private final ConcurrentMap<Class<? extends UserExpression>, Integer> registeredClasses = new ConcurrentHashMap<>();

            public void registerUserExpressionClass(Class<? extends UserExpression> expressionClass,
                                                    UserExpression.Deserializer deserializer)
            {
                int id = registeredClasses.computeIfAbsent(expressionClass, (cls) -> counter.getAndIncrement());
                deserializers.put(id, deserializer);

                logger.debug("Registered user defined expression type {} and serializer {} with identifier {}",
                             expressionClass.getName(), deserializer.getClass().getName(), id);
            }

            public Integer getId(UserExpression expression)
            {
                return registeredClasses.get(expression.getClass());
            }

            public Deserializer getDeserializer(int id)
            {
                return deserializers.get(id);
            }
        }

        protected static abstract class Deserializer
        {
            protected abstract UserExpression deserialize(DataInputPlus in,
                                                          int version,
                                                          TableMetadata metadata) throws IOException;
        }

        public static void register(Class<? extends UserExpression> expressionClass, Deserializer deserializer)
        {
            deserializers.registerUserExpressionClass(expressionClass, deserializer);
        }

        private static UserExpression deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            int id = in.readInt();
            Deserializer deserializer = deserializers.getDeserializer(id);
            assert deserializer != null : "No user defined expression type registered with id " + id;
            return deserializer.deserialize(in, version, metadata);
        }

        private static void serialize(UserExpression expression, DataOutputPlus out, int version) throws IOException
        {
            Integer id = deserializers.getId(expression);
            assert id != null : "User defined expression type " + expression.getClass().getName() + " is not registered";
            out.writeInt(id);
            expression.serialize(out, version);
        }

        private static long serializedSize(UserExpression expression, int version)
        {   // 4 bytes for the expression type id
            return 4 + expression.serializedSize(version);
        }

        protected UserExpression(ColumnMetadata column, Operator operator, ByteBuffer value)
        {
            super(column, operator, value);
        }

        protected Kind kind()
        {
            return Kind.USER;
        }

        protected abstract void serialize(DataOutputPlus out, int version) throws IOException;
        protected abstract long serializedSize(int version);
    }

    public static class Serializer
    {
        public void serialize(RowFilter filter, DataOutputPlus out, int version) throws IOException
        {
            // Only VERSION_AXON_50 peers understand the tree format, so older peers get the legacy
            // flat format, which cannot represent a disjunction. Flattening one would decode as a
            // conjunction on the peer and silently under select, so we throw instead. The statement
            // layer refuses disjunctive queries before dispatch, this is the backstop.
            if (version >= MessagingService.VERSION_AXON_50)
            {
                serialize(filter.root, out, version);
                return;
            }

            if (filter.containsDisjunction())
                throw new IllegalStateException("Cannot serialize a disjunctive row filter to a node on messaging version " + version);

            out.writeBoolean(false); // Old "is for thrift" boolean
            out.writeUnsignedVInt32(filter.root.expressions().size());
            for (Expression expr : filter.root.expressions())
                Expression.serializer.serialize(expr, out, version);

        }

        private void serialize(FilterElement element, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(element.isDisjunction() ? 1 : 0);
            out.writeUnsignedVInt32(element.expressions().size());
            for (Expression expr : element.expressions())
                Expression.serializer.serialize(expr, out, version);
            out.writeUnsignedVInt32(element.children().size());
            for (FilterElement child : element.children())
                serialize(child, out, version);
        }

        public RowFilter deserialize(DataInputPlus in, int version, TableMetadata metadata, boolean needsReconciliation) throws IOException
        {
            if (version >= MessagingService.VERSION_AXON_50)
                return new RowFilter(deserializeElement(in, version, metadata), needsReconciliation);

            in.readBoolean(); // Unused
            int size = in.readUnsignedVInt32();
            List<Expression> expressions = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
                expressions.add(Expression.serializer.deserialize(in, version, metadata));

            return new RowFilter(new FilterElement(false, expressions, new ArrayList<>()), needsReconciliation);
        }

        private FilterElement deserializeElement(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            boolean isDisjunction = (in.readByte() & 1) == 1;
            int expressionCount = in.readUnsignedVInt32();
            List<Expression> expressions = new ArrayList<>(expressionCount);
            for (int i = 0; i < expressionCount; i++)
                expressions.add(Expression.serializer.deserialize(in, version, metadata));
            int childCount = in.readUnsignedVInt32();
            List<FilterElement> children = new ArrayList<>(childCount);
            for (int i = 0; i < childCount; i++)
                children.add(deserializeElement(in, version, metadata));
            return new FilterElement(isDisjunction, expressions, children);
        }

        public long serializedSize(RowFilter filter, int version)
        {
            if (version >= MessagingService.VERSION_AXON_50)
                return serializedSize(filter.root, version);

            long size = 1 // unused boolean
                      + TypeSizes.sizeofUnsignedVInt(filter.root.expressions().size());
            for (Expression expr : filter.root.expressions())
                size += Expression.serializer.serializedSize(expr, version);
            return size;
        }

        private long serializedSize(FilterElement element, int version)
        {
            long size = 1 // flags byte
                      + TypeSizes.sizeofUnsignedVInt(element.expressions().size());
            for (Expression expr : element.expressions())
                size += Expression.serializer.serializedSize(expr, version);
            size += TypeSizes.sizeofUnsignedVInt(element.children().size());
            for (FilterElement child : element.children())
                size += serializedSize(child, version);
            return size;
        }
    }
}
