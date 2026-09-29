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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.analyzer.AnalyzedToken;
import org.apache.cassandra.index.sai.disk.v1.vector.PrimaryKeyWithScore;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeUnionIterator;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CloseableIterator;

public class Operation
{
    public enum BooleanOperator
    {
        AND((a, b) -> a & b),
        OR((a, b) -> a | b);

        private final BiFunction<Boolean, Boolean, Boolean> func;

        BooleanOperator(BiFunction<Boolean, Boolean, Boolean> func)
        {
            this.func = func;
        }

        public boolean apply(boolean a, boolean b)
        {
            return func.apply(a, b);
        }
    }

    public static class Expressions
    {
        final ListMultimap<ColumnMetadata, Expression> expressions;
        final Set<ColumnMetadata> unindexedColumns;

        Expressions(ListMultimap<ColumnMetadata, Expression> expressions, Set<ColumnMetadata> unindexedColumns)
        {
            this.expressions = expressions;
            this.unindexedColumns = unindexedColumns;
        }

        Set<ColumnMetadata> columns()
        {
            return expressions.keySet();
        }

        Collection<Expression> all()
        {
            return expressions.values();
        }

        List<Expression> expressionsFor(ColumnMetadata column)
        {
            return expressions.get(column);
        }

        boolean isEmpty()
        {
            return expressions.isEmpty();
        }

        int size()
        {
            return expressions.size();
        }

        boolean isUnindexed(ColumnMetadata column)
        {
            return unindexedColumns.contains(column);
        }

        boolean hasMultipleUnindexedColumns()
        {
            return unindexedColumns.size() > 1;
        }

        int unindexedColumnCount()
        {
            return unindexedColumns.size();
        }
    }

    @VisibleForTesting
    protected static Expressions buildIndexExpressions(QueryController queryController, List<RowFilter.Expression> expressions)
    {
        ListMultimap<ColumnMetadata, Expression> analyzed = ArrayListMultimap.create();
        Set<ColumnMetadata> unindexedColumns = Collections.emptySet();

        // sort all the expressions in the operation by name and priority of the logical operator
        // this gives us an efficient way to handle inequality and combining into ranges without extra processing
        // and converting expressions from one type to another.
        expressions.sort((a, b) -> {
            int cmp = a.column().compareTo(b.column());
            return cmp == 0 ? -Integer.compare(getPriority(a.operator()), getPriority(b.operator())) : cmp;
        });

        for (final RowFilter.Expression expression : expressions)
        {
            if (Expression.supportsOperator(expression.operator()))
            {
                StorageAttachedIndex index = queryController.indexFor(expression);
                List<Expression> perColumn = analyzed.get(expression.column());

                if (index == null)
                {
                    buildUnindexedExpression(queryController, expression, perColumn);

                    if (!expression.column().isPrimaryKeyColumn())
                    {
                        if (unindexedColumns.isEmpty())
                            unindexedColumns = new HashSet<>(3);

                        unindexedColumns.add(expression.column());
                    }
                }
                else
                {
                    buildIndexedExpression(index, expression, perColumn);
                }
            }
        }

        return new Expressions(analyzed, unindexedColumns);
    }

    private static void buildUnindexedExpression(QueryController queryController,
                                                 RowFilter.Expression expression,
                                                 List<Expression> perColumn)
    {
        IndexTermType indexTermType = IndexTermType.create(expression.column(),
                                                           queryController.metadata().partitionKeyColumns(),
                                                           determineIndexTargetType(expression));
        if (indexTermType.isMultiExpression(expression))
        {
            perColumn.add(Expression.create(indexTermType).add(expression.operator(), expression.getIndexValue().duplicate()));
        }
        else
        {
            Expression range;
            if (perColumn.size() == 0)
            {
                range = Expression.create(indexTermType);
                perColumn.add(range);
            }
            else
            {
                range = Iterables.getLast(perColumn);
            }
            range.add(expression.operator(), expression.getIndexValue().duplicate());
        }
    }

    private static void buildIndexedExpression(StorageAttachedIndex index, RowFilter.Expression expression, List<Expression> perColumn)
    {
        if (index.hasLuceneAnalyzer() && expression.operator().isAnalyzedPhrase())
        {
            List<AnalyzedToken> tokens = index.luceneQueryAnalyzer().analyze(expression.getIndexValue().duplicate());
            traceQueryTokens(index, expression, tokens);
            perColumn.add(Expression.create(index).phrase(tokens));
        }
        else if (index.hasLuceneAnalyzer() && (expression.operator().isAnalyzedMatch()
                                               || index.termType().isMultiExpression(expression)))
        {
            // One Expression per distinct query analyzer token, AND semantics downstream. This shape
            // must never fold tokens into one EQ range, whose bounds overwrite each other.
            List<AnalyzedToken> tokens = index.luceneQueryAnalyzer().analyze(expression.getIndexValue().duplicate());
            traceQueryTokens(index, expression, tokens);

            Set<ByteBuffer> distinctTokens = new LinkedHashSet<>();
            for (AnalyzedToken token : tokens)
                distinctTokens.add(token.bytes());

            if (distinctTokens.isEmpty())
            {
                // A query the analyzer emits no tokens for, e.g. only stopwords, matches nothing
                perColumn.add(Expression.create(index).matchingNothing(expression.getIndexValue().duplicate()));
            }
            else
            {
                Operator operator = expression.operator() == Operator.EQ ? Operator.ANALYZER_MATCHES
                                                                         : expression.operator();
                for (ByteBuffer token : distinctTokens)
                    perColumn.add(Expression.create(index).add(operator, token.duplicate()));
            }
        }
        else if (index.hasAnalyzer())
        {
            AbstractAnalyzer analyzer = index.analyzer();
            try
            {
                analyzer.reset(expression.getIndexValue().duplicate());

                if (index.termType().isMultiExpression(expression))
                {
                    while (analyzer.hasNext())
                    {
                        final ByteBuffer token = analyzer.next();
                        perColumn.add(Expression.create(index).add(expression.operator(), token.duplicate()));
                    }
                }
                else
                // "range" or not-equals operator, combines both bounds together into the single expression,
                // if operation of the group is AND, otherwise we are forced to create separate expressions,
                // not-equals is combined with the range iff operator is AND.
                {
                    Expression range;
                    if (perColumn.size() == 0)
                    {
                        range = Expression.create(index);
                        perColumn.add(range);
                    }
                    else
                    {
                        range = Iterables.getLast(perColumn);
                    }

                    if (index.termType().isLiteral())
                    {
                        while (analyzer.hasNext())
                        {
                            ByteBuffer term = analyzer.next();
                            range.add(expression.operator(), term.duplicate());
                        }
                    }
                    else
                    {
                        range.add(expression.operator(), expression.getIndexValue().duplicate());
                    }
                }
            }
            finally
            {
                analyzer.end();
            }
        }
        else
        {
            if (index.termType().isMultiExpression(expression))
            {
                perColumn.add(Expression.create(index).add(expression.operator(), expression.getIndexValue().duplicate()));
            }
            else
            {
                Expression range;
                if (perColumn.size() == 0)
                {
                    range = Expression.create(index);
                    perColumn.add(range);
                }
                else
                {
                    range = Iterables.getLast(perColumn);
                }
                range.add(expression.operator(), expression.getIndexValue().duplicate());
            }
        }
    }

    /**
     * Traces how the query analyzer tokenized the queried value, one event per analyzed expression.
     */
    private static void traceQueryTokens(StorageAttachedIndex index, RowFilter.Expression expression, List<AnalyzedToken> tokens)
    {
        if (!Tracing.isTracing())
            return;

        StringBuilder builder = new StringBuilder();
        for (AnalyzedToken token : tokens)
        {
            if (builder.length() > 0)
                builder.append(", ");
            builder.append(index.termType().asString(token.bytes())).append('@').append(token.position());
        }
        Tracing.trace("Query analyzed {} {} value into {} tokens with positions: [{}]",
                      expression.column().name, expression.operator(), tokens.size(), builder);
    }

    /**
     * Determines the {@link IndexTarget.Type} for the expression. In this case we are only interested in map types and
     * the operator being used in the expression.
     */
    private static IndexTarget.Type determineIndexTargetType(RowFilter.Expression expression)
    {
        AbstractType<?> type  = expression.column().type;
        IndexTarget.Type indexTargetType = IndexTarget.Type.SIMPLE;
        if (type.isCollection() && type.isMultiCell())
        {
            CollectionType<?> collection = ((CollectionType<?>) type);
            if (collection.kind == CollectionType.Kind.MAP)
            {
                switch (expression.operator())
                {
                    case EQ:
                        indexTargetType = IndexTarget.Type.KEYS_AND_VALUES;
                        break;
                    case CONTAINS:
                        indexTargetType = IndexTarget.Type.VALUES;
                        break;
                    case CONTAINS_KEY:
                        indexTargetType = IndexTarget.Type.KEYS;
                        break;
                    case ANALYZER_MATCHES:
                    case PHRASE:
                        indexTargetType = IndexTarget.Type.VALUES;
                        break;
                    case ANALYZER_MATCHES_KEY:
                    case PHRASE_KEY:
                        indexTargetType = IndexTarget.Type.KEYS;
                        break;
                    default:
                        throw new InvalidRequestException("Invalid operator");
                }
            }
        }
        return indexTargetType;
    }

    private static int getPriority(Operator op)
    {
        switch (op)
        {
            case EQ:
            case CONTAINS:
            case CONTAINS_KEY:
                return 5;

            case GTE:
            case GT:
                return 3;

            case LTE:
            case LT:
                return 2;

            default:
                return 0;
        }
    }

    /**
     * Converts expressions into filter tree for query.
     *
     * @return a KeyRangeIterator over the index query results
     */
    static KeyRangeIterator buildIterator(QueryController controller)
    {
        Node node = Node.buildTree(controller.indexFilter()).analyzeTree(controller);
        // Scoped to disjunctions so queries without OR keep their existing trace output
        if (Tracing.isTracing() && controller.indexFilter().containsDisjunction())
            Tracing.trace("Executing index query tree {}", node.treeDescription());
        return node.rangeIterator(controller);
    }

    /**
     * Converts expressions into filter tree for query.
     *
     * @return a KeyRangeIterator over the index query results
     */
    static CloseableIterator<PrimaryKeyWithScore> buildIteratorForOrder(QueryController controller, QueryViewBuilder.QueryExpressionView view)
    {
        if (controller.indexFilter().getExpressions().size() == 1)
            // If we only have one expression, we just use the ANN index to order and limit.
            return controller.getTopKRows(view);

        // Otherwise, we need to search first, then order.
        KeyRangeIterator iterator = buildIterator(controller);
        return controller.getTopKRows(iterator, view);
    }

    /**
     * Converts expressions into a filter tree mirroring the {@link RowFilter} tree shape.
     * <p>
     * Filter tree allows us to do a couple of important optimizations
     * namely, group flattening for AND operations (query rewrite), expression bounds checks,
     * "satisfies by" checks for resulting rows with an early exit.
     *
     * @param forceStrict when true every node filters strictly, used by the coordinator re-check
     * over merged rows. When false each AND node applies its own per node strictness computed
     * from the row filter tree.
     *
     * @return root of the filter tree.
     */
    static FilterTree buildFilter(QueryController controller, boolean forceStrict)
    {
        return Node.buildTree(controller.indexFilter()).buildFilter(controller, forceStrict);
    }

    static abstract class Node
    {
        Expressions expressions;

        boolean canFilter()
        {
            return (expressions != null && !expressions.isEmpty()) || !children().isEmpty();
        }

        List<Node> children()
        {
            return Collections.emptyList();
        }

        void add(Node child)
        {
            throw new UnsupportedOperationException();
        }

        RowFilter.Expression expression()
        {
            throw new UnsupportedOperationException();
        }

        abstract void analyze(List<RowFilter.Expression> expressionList, QueryController controller);

        abstract FilterTree filterTree(boolean forceStrict, QueryContext context);

        abstract KeyRangeIterator rangeIterator(QueryController controller);

        static Node buildTree(RowFilter filterOperation)
        {
            return buildTree(filterOperation.root(), filterOperation.needsReconciliation());
        }

        private static Node buildTree(RowFilter.FilterElement element, boolean needsReconciliation)
        {
            OperatorNode node;
            if (element.isDisjunction())
            {
                node = new OrNode();
                // Every direct leaf of a disjunction gets its own AND branch node, so that no
                // analysis can ever pool or fold expressions across OR branches. A same column
                // pair like x < 5 OR x > 10 folded into one range would produce an empty match.
                for (RowFilter.Expression expression : element.expressions())
                {
                    AndNode branch = new AndNode(true);
                    branch.add(buildExpression(expression));
                    node.add(branch);
                }
            }
            else
            {
                // A conjunction intersecting several mutable columns cannot be evaluated
                // strictly on a replica when reads need reconciliation (CASSANDRA-19018)
                node = new AndNode(!(needsReconciliation && element.isMutableIntersection()));
                for (RowFilter.Expression expression : element.expressions())
                    node.add(buildExpression(expression));
            }

            for (RowFilter.FilterElement child : element.children())
                node.add(buildTree(child, needsReconciliation));

            return node;
        }

        static Node buildExpression(RowFilter.Expression expression)
        {
            return new ExpressionNode(expression);
        }

        Node analyzeTree(QueryController controller)
        {
            List<RowFilter.Expression> expressionList = new ArrayList<>();
            doTreeAnalysis(this, expressionList, controller);
            if (!expressionList.isEmpty())
                this.analyze(expressionList, controller);
            return this;
        }

        void doTreeAnalysis(Node node, List<RowFilter.Expression> expressions, QueryController controller)
        {
            if (node.children().isEmpty())
                expressions.add(node.expression());
            else
            {
                List<RowFilter.Expression> expressionList = new ArrayList<>();
                for (Node child : node.children())
                    doTreeAnalysis(child, expressionList, controller);
                node.analyze(expressionList, controller);
            }
        }

        FilterTree buildFilter(QueryController controller, boolean forceStrict)
        {
            analyzeTree(controller);
            return doBuildFilter(forceStrict, controller.queryContext);
        }

        private FilterTree doBuildFilter(boolean forceStrict, QueryContext context)
        {
            FilterTree tree = filterTree(forceStrict, context);
            for (Node child : children())
                if (child.canFilter())
                    tree.addChild(child.doBuildFilter(forceStrict, context));
            return tree;
        }

        /**
         * @return a compact description of the tree shape for tracing, operators and expression
         * counts only, never values
         */
        String treeDescription()
        {
            StringBuilder sb = new StringBuilder();
            describe(sb);
            return sb.toString();
        }

        void describe(StringBuilder sb)
        {
            sb.append(getClass().getSimpleName())
              .append('[')
              .append(expressions == null ? 0 : expressions.size())
              .append(']');
            if (!children().isEmpty())
            {
                sb.append('(');
                for (int i = 0; i < children().size(); i++)
                {
                    if (i > 0)
                        sb.append(", ");
                    children().get(i).describe(sb);
                }
                sb.append(')');
            }
        }
    }

    static abstract class OperatorNode extends Node
    {
        final List<Node> children = new ArrayList<>();

        @Override
        public List<Node> children()
        {
            return children;
        }

        @Override
        public void add(Node child)
        {
            children.add(child);
        }
    }

    static class AndNode extends OperatorNode
    {
        /**
         * Whether this node's intersection may be evaluated strictly on this replica, see
         * {@link RowFilter.FilterElement#isMutableIntersection()}.
         */
        private final boolean strict;

        AndNode(boolean strict)
        {
            this.strict = strict;
        }

        @Override
        public void analyze(List<RowFilter.Expression> expressionList, QueryController controller)
        {
            expressions = buildIndexExpressions(controller, expressionList);
        }

        @Override
        FilterTree filterTree(boolean forceStrict, QueryContext context)
        {
            if (needsUnionOnReplica(forceStrict))
                return new FilterTree(BooleanOperator.OR, expressions, true, context);

            return new FilterTree(BooleanOperator.AND, expressions, forceStrict || strict, context);
        }

        /**
         * A disjunction child is searched strictly, so its matches never set
         * {@link QueryContext#hasUnrepairedMatches}. Like an unindexed column it is a conjunct whose match
         * does not mark the read as holding unrepaired matches. When a node has a disjunction child and two or more such
         * conjuncts, their newest matching values can sit on different replicas. The replica then keeps
         * a row that satisfies any one conjunct, and the coordinator re-check filters the merged row
         * strictly. Never applies to strict nodes, to the coordinator re-check, or to a node without a
         * disjunction child.
         */
        private boolean needsUnionOnReplica(boolean forceStrict)
        {
            if (forceStrict || strict)
                return false;

            int disjunctions = 0;
            for (Node child : children)
                if (child instanceof OrNode)
                    disjunctions++;

            return disjunctions > 0 && disjunctions + expressions.unindexedColumnCount() >= 2;
        }

        @Override
        KeyRangeIterator rangeIterator(QueryController controller)
        {
            KeyRangeIterator.Builder builder = controller.getIndexQueryResults(expressions.all(), strict);
            for (Node child : children)
            {
                boolean canFilter = child.canFilter();
                if (canFilter)
                    builder.add(child.rangeIterator(controller));
            }
            return builder.build();
        }
    }

    static class OrNode extends OperatorNode
    {
        @Override
        public void analyze(List<RowFilter.Expression> expressionList, QueryController controller)
        {
            // Direct leaves are wrapped in their own AND branch nodes at build time, so a
            // disjunction never pools expressions of its own
            assert expressionList.isEmpty() : "Disjunction nodes should not pool expressions";
            expressions = buildIndexExpressions(controller, expressionList);
        }

        @Override
        FilterTree filterTree(boolean forceStrict, QueryContext context)
        {
            // A union only widens results, which is the safe direction under reconciliation,
            // so a disjunction never needs the strictness downgrade
            return new FilterTree(BooleanOperator.OR, expressions, true, context);
        }

        @Override
        KeyRangeIterator rangeIterator(QueryController controller)
        {
            KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder(children.size());
            for (Node child : children)
                if (child.canFilter())
                    builder.add(child.rangeIterator(controller));
            return builder.build();
        }
    }

    static class ExpressionNode extends Node
    {
        final RowFilter.Expression expression;

        @Override
        public void analyze(List<RowFilter.Expression> expressionList, QueryController controller)
        {
            expressions = buildIndexExpressions(controller, expressionList);
            assert expressions.size() == 1 : "Expression nodes should only have a single expression!";
        }

        @Override
        FilterTree filterTree(boolean forceStrict, QueryContext context)
        {
            // There should only be one expression, so AND/OR would both work here.
            return new FilterTree(BooleanOperator.AND, expressions, true, context);
        }

        public ExpressionNode(RowFilter.Expression expression)
        {
            this.expression = expression;
        }

        @Override
        public RowFilter.Expression expression()
        {
            return expression;
        }

        @Override
        KeyRangeIterator rangeIterator(QueryController controller)
        {
            assert canFilter() : "Cannot process query with no expressions";

            return controller.getIndexQueryResults(expressions.all(), true).build();
        }
    }
}
