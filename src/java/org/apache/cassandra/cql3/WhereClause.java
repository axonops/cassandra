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
package org.apache.cassandra.cql3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.cql3.restrictions.CustomIndexExpression;

/**
 * The WHERE clause of a statement, represented as a tree of {@link ExpressionElement}s.
 * <p>
 * The root is always a conjunction whose children are leaves ({@link RelationElement},
 * {@link CustomIndexExpressionElement}) or {@link OrElement} subtrees, and nested containers
 * with the same operator are flattened at build time. Clauses without OR therefore have a
 * root whose children are all leaves, matching the flat shape this class used to have.
 */
public final class WhereClause
{
    private static final WhereClause EMPTY = new WhereClause(new Builder());

    private final AndElement root;

    private WhereClause(Builder builder)
    {
        root = builder.buildRoot();
    }

    private WhereClause(AndElement root)
    {
        this.root = root;
    }

    public static WhereClause empty()
    {
        return EMPTY;
    }

    /**
     * @return the root of the expression tree, always a conjunction
     */
    public AndElement root()
    {
        return root;
    }

    /**
     * @return true if any node of the tree is a disjunction
     */
    public boolean containsDisjunction()
    {
        return root.containsDisjunction();
    }

    /**
     * Flat view over the relation leaves, only defined for clauses without OR where the
     * whole clause is one conjunction.
     */
    public List<Relation> relations()
    {
        if (containsDisjunction())
            throw new AssertionError("The flat relations view is not defined for a WHERE clause containing OR");

        List<Relation> relations = new ArrayList<>(root.children().size());
        for (ExpressionElement element : root.children())
            if (element instanceof RelationElement)
                relations.add(((RelationElement) element).relation());
        return relations;
    }

    /**
     * Flat view over the custom index expression leaves, only defined for clauses without OR.
     */
    public List<CustomIndexExpression> expressions()
    {
        if (containsDisjunction())
            throw new AssertionError("The flat expressions view is not defined for a WHERE clause containing OR");

        List<CustomIndexExpression> expressions = new ArrayList<>();
        for (ExpressionElement element : root.children())
            if (element instanceof CustomIndexExpressionElement)
                expressions.add(((CustomIndexExpressionElement) element).expression());
        return expressions;
    }

    public boolean containsCustomExpressions()
    {
        return root.containsCustomExpressions();
    }

    /**
     * Checks if the where clause contains some token relations.
     *
     * @return {@code true} if it is the case, {@code false} otherwise.
     */
    public boolean containsTokenRelations()
    {
        return root.containsTokenRelations();
    }

    /**
     * Renames identifiers in all relations
     * @param from the old identifier
     * @param to the new identifier
     * @return a new WhereClause with with "from" replaced by "to" in all relations
     */
    public WhereClause renameIdentifier(ColumnIdentifier from, ColumnIdentifier to)
    {
        return new WhereClause((AndElement) root.renameIdentifier(from, to));
    }

    public static WhereClause parse(String cql) throws RecognitionException
    {
        return CQLFragmentParser.parseAnyUnhandled(CqlParser::whereClause, cql).build();
    }

    @Override
    public String toString()
    {
        return toCQLString();
    }

    /**
     * Returns a CQL representation of this WHERE clause.
     *
     * @return a CQL representation of this WHERE clause
     */
    public String toCQLString()
    {
        return root.toCQLString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof WhereClause))
            return false;

        WhereClause wc = (WhereClause) o;
        return root.equals(wc.root);
    }

    @Override
    public int hashCode()
    {
        return root.hashCode();
    }

    /**
     * A node of the WHERE clause expression tree.
     */
    public static abstract class ExpressionElement
    {
        /**
         * @return the given elements as a conjunction, flattening nested conjunctions. A single
         * element is returned as itself.
         */
        public static ExpressionElement and(List<ExpressionElement> elements)
        {
            return elements.size() == 1 ? elements.get(0) : new AndElement(elements);
        }

        /**
         * @return the given elements as a disjunction, flattening nested disjunctions. A single
         * element is returned as itself.
         */
        public static ExpressionElement or(List<ExpressionElement> elements)
        {
            return elements.size() == 1 ? elements.get(0) : new OrElement(elements);
        }

        public boolean containsDisjunction()
        {
            return false;
        }

        public boolean containsCustomExpressions()
        {
            return false;
        }

        public boolean containsTokenRelations()
        {
            return false;
        }

        public abstract ExpressionElement renameIdentifier(ColumnIdentifier from, ColumnIdentifier to);

        public abstract String toCQLString();

        @Override
        public String toString()
        {
            return toCQLString();
        }
    }

    public static final class RelationElement extends ExpressionElement
    {
        private final Relation relation;

        public RelationElement(Relation relation)
        {
            this.relation = relation;
        }

        public Relation relation()
        {
            return relation;
        }

        @Override
        public boolean containsTokenRelations()
        {
            return relation.onToken();
        }

        @Override
        public ExpressionElement renameIdentifier(ColumnIdentifier from, ColumnIdentifier to)
        {
            return new RelationElement(relation.renameIdentifier(from, to));
        }

        @Override
        public String toCQLString()
        {
            return relation.toCQLString();
        }

        @Override
        public boolean equals(Object o)
        {
            return o instanceof RelationElement && relation.equals(((RelationElement) o).relation);
        }

        @Override
        public int hashCode()
        {
            return relation.hashCode();
        }
    }

    public static final class CustomIndexExpressionElement extends ExpressionElement
    {
        private final CustomIndexExpression expression;

        public CustomIndexExpressionElement(CustomIndexExpression expression)
        {
            this.expression = expression;
        }

        public CustomIndexExpression expression()
        {
            return expression;
        }

        @Override
        public boolean containsCustomExpressions()
        {
            return true;
        }

        @Override
        public ExpressionElement renameIdentifier(ColumnIdentifier from, ColumnIdentifier to)
        {
            return this;
        }

        @Override
        public String toCQLString()
        {
            return expression.toCQLString();
        }

        @Override
        public boolean equals(Object o)
        {
            return o instanceof CustomIndexExpressionElement && expression.equals(((CustomIndexExpressionElement) o).expression);
        }

        @Override
        public int hashCode()
        {
            return expression.hashCode();
        }
    }

    public static abstract class ContainerElement extends ExpressionElement
    {
        private final List<ExpressionElement> children;

        private ContainerElement(List<ExpressionElement> children)
        {
            // Flatten nested containers with the same operator, so AND(a, AND(b, c))
            // becomes AND(a, b, c)
            List<ExpressionElement> flattened = new ArrayList<>(children.size());
            for (ExpressionElement child : children)
            {
                if (child.getClass() == getClass())
                    flattened.addAll(((ContainerElement) child).children);
                else
                    flattened.add(child);
            }
            this.children = Collections.unmodifiableList(flattened);
        }

        public List<ExpressionElement> children()
        {
            return children;
        }

        abstract String operatorName();

        abstract ContainerElement withChildren(List<ExpressionElement> children);

        @Override
        public boolean containsDisjunction()
        {
            for (ExpressionElement child : children)
                if (child.containsDisjunction())
                    return true;
            return false;
        }

        @Override
        public boolean containsCustomExpressions()
        {
            for (ExpressionElement child : children)
                if (child.containsCustomExpressions())
                    return true;
            return false;
        }

        @Override
        public boolean containsTokenRelations()
        {
            for (ExpressionElement child : children)
                if (child.containsTokenRelations())
                    return true;
            return false;
        }

        @Override
        public ExpressionElement renameIdentifier(ColumnIdentifier from, ColumnIdentifier to)
        {
            List<ExpressionElement> renamed = new ArrayList<>(children.size());
            for (ExpressionElement child : children)
                renamed.add(child.renameIdentifier(from, to));
            return withChildren(renamed);
        }

        @Override
        public String toCQLString()
        {
            StringBuilder sb = new StringBuilder();
            for (ExpressionElement child : children)
            {
                if (sb.length() > 0)
                    sb.append(' ').append(operatorName()).append(' ');

                if (child instanceof ContainerElement)
                    sb.append('(').append(child.toCQLString()).append(')');
                else
                    sb.append(child.toCQLString());
            }
            return sb.toString();
        }

        @Override
        public boolean equals(Object o)
        {
            return o != null && getClass() == o.getClass() && children.equals(((ContainerElement) o).children);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(getClass(), children);
        }
    }

    public static final class AndElement extends ContainerElement
    {
        public AndElement(List<ExpressionElement> children)
        {
            super(children);
        }

        @Override
        String operatorName()
        {
            return "AND";
        }

        @Override
        ContainerElement withChildren(List<ExpressionElement> children)
        {
            return new AndElement(children);
        }
    }

    public static final class OrElement extends ContainerElement
    {
        public OrElement(List<ExpressionElement> children)
        {
            super(children);
        }

        @Override
        public boolean containsDisjunction()
        {
            return true;
        }

        @Override
        String operatorName()
        {
            return "OR";
        }

        @Override
        ContainerElement withChildren(List<ExpressionElement> children)
        {
            return new OrElement(children);
        }
    }

    public static final class Builder
    {
        private final List<ExpressionElement> elements = new ArrayList<>();
        private ExpressionElement parsedRoot;

        public Builder add(Relation relation)
        {
            elements.add(new RelationElement(relation));
            return this;
        }

        public Builder add(CustomIndexExpression expression)
        {
            elements.add(new CustomIndexExpressionElement(expression));
            return this;
        }

        /**
         * Sets the tree parsed by the grammar. Never combined with {@link #add}, which exists
         * for programmatic conjunction-only construction.
         */
        public Builder root(ExpressionElement root)
        {
            parsedRoot = root;
            return this;
        }

        /**
         * Removes and returns the single element accumulated by a grammar sub-rule, used by the
         * parser to turn one parsed relation into a tree leaf. A recognition error may have left
         * nothing behind, in which case this returns null and the parser surfaces the syntax
         * error once the rule actions have run.
         */
        ExpressionElement extractParsed()
        {
            return elements.isEmpty() ? null : elements.remove(0);
        }

        AndElement buildRoot()
        {
            ExpressionElement element = parsedRoot != null ? parsedRoot : ExpressionElement.and(new ArrayList<>(elements));
            if (element instanceof AndElement)
                return (AndElement) element;
            return new AndElement(Collections.singletonList(element));
        }

        public WhereClause build()
        {
            return new WhereClause(this);
        }
    }
}
