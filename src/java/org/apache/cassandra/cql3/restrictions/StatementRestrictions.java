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
package org.apache.cassandra.cql3.restrictions;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.cql3.statements.StatementType;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.btree.BTreeSet;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * The restrictions corresponding to the relations specified on the where-clause of CQL query.
 */
public final class StatementRestrictions
{
    private static final String ALLOW_FILTERING_MESSAGE =
            "Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. ";

    public static final String REQUIRES_ALLOW_FILTERING_MESSAGE = ALLOW_FILTERING_MESSAGE +
            "If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING";

    public static final String CANNOT_USE_ALLOW_FILTERING_MESSAGE = ALLOW_FILTERING_MESSAGE +
            "Executing this query despite the performance unpredictability with ALLOW FILTERING has been disabled " +
            "by the allow_filtering_enabled property in cassandra.yaml";

    public static final String ANN_REQUIRES_INDEX_MESSAGE = "ANN ordering by vector requires the column to be indexed";

    public static final String VECTOR_INDEXES_ANN_ONLY_MESSAGE = "Vector indexes only support ANN queries";

    public static final String ANN_ONLY_SUPPORTED_ON_VECTOR_MESSAGE = "ANN ordering is only supported on float vector indexes";

    public static final String ANN_REQUIRES_INDEXED_FILTERING_MESSAGE = "ANN ordering by vector requires all restricted column(s) to be indexed";

    public static final String ANALYZED_OPERATOR_REQUIRES_INDEX_MESSAGE =
            "%s is only supported on columns with a storage-attached index using an index_analyzer. %s is not valid.";

    public static final String ANALYZED_CONTAINS_MESSAGE =
            "Column '%s' has an analyzed index: CONTAINS compares whole elements, use MATCH or PHRASE for word search.";

    public static final String ANALYZED_CONTAINS_KEY_MESSAGE =
            "Column '%s' has an analyzed index on its keys: CONTAINS KEY compares whole keys, use MATCH KEY or PHRASE KEY for word search.";

    public static final String MAP_KEYS_OPERATOR_REQUIRES_INDEX_MESSAGE = "%s needs an index with an index_analyzer on KEYS(%s).";

    public static final String VALUE_CHANGING_SASI_NEXT_TO_OR_MESSAGE =
        "Column %s has a SASI index that changes values with its analyzer. A condition on it cannot be combined with OR, " +
        "because only that index can check it and that index does not support OR.";

    /**
     * The type of statement
     */
    private final StatementType type;

    /**
     * The Column Family meta data
     */
    public final TableMetadata table;

    /**
     * Restrictions on partitioning columns
     */
    private PartitionKeyRestrictions partitionKeyRestrictions;

    /**
     * Restrictions on clustering columns
     */
    private ClusteringColumnRestrictions clusteringColumnsRestrictions;

    /**
     * Restriction on non-primary key columns (i.e. secondary index restrictions)
     */
    private RestrictionSet nonPrimaryKeyRestrictions;

    private Set<ColumnMetadata> notNullColumns;

    /**
     * The restrictions used to build the row filter
     */
    private final IndexRestrictions filterRestrictions = new IndexRestrictions();

    /**
     * The OR subtrees of the WHERE clause, one holder per root level disjunction. Their leaf
     * restrictions are validated and used to build the row filter tree, but they never enter
     * the conjunctive merge machinery above.
     */
    private final List<DisjunctionHolder> disjunctions = new ArrayList<>();

    /**
     * Whether the disjunctions must be evaluated by filtering because some leaf is not supported
     * by an index group that understands OR.
     */
    private boolean disjunctionsNeedFiltering;

    /**
     * <code>true</code> if the secondary index need to be queried, <code>false</code> otherwise
     */
    private boolean usesSecondaryIndexing;

    /**
     * Specify if the query will return a range of partition keys.
     */
    private boolean isKeyRange;

    /**
     * <code>true</code> if nonPrimaryKeyRestrictions contains restriction on a regular column,
     * <code>false</code> otherwise.
     */
    private boolean hasRegularColumnsRestrictions;

    /**
     * Creates a new empty <code>StatementRestrictions</code>.
     *
     * @param type the type of statement
     * @param table the column family meta data
     * @return a new empty <code>StatementRestrictions</code>.
     */
    public static StatementRestrictions empty(StatementType type, TableMetadata table)
    {
        return new StatementRestrictions(type, table, false);
    }

    private StatementRestrictions(StatementType type, TableMetadata table, boolean allowFiltering)
    {
        this.type = type;
        this.table = table;
        this.partitionKeyRestrictions = new PartitionKeySingleRestrictionSet(table.partitionKeyAsClusteringComparator());
        this.clusteringColumnsRestrictions = new ClusteringColumnRestrictions(table, allowFiltering);
        this.nonPrimaryKeyRestrictions = new RestrictionSet();
        this.notNullColumns = new HashSet<>();
    }

    public StatementRestrictions(ClientState state,
                                 StatementType type,
                                 TableMetadata table,
                                 WhereClause whereClause,
                                 VariableSpecifications boundNames,
                                 List<Ordering> orderings,
                                 boolean selectsOnlyStaticColumns,
                                 boolean allowFiltering,
                                 boolean forView)
    {
        this(state, type, table, whereClause, boundNames, orderings, selectsOnlyStaticColumns, type.allowUseOfSecondaryIndices(), allowFiltering, forView);
    }

    /*
     * We want to override allowUseOfSecondaryIndices flag from the StatementType for MV statements
     * to avoid initing the Keyspace and SecondaryIndexManager.
     */
    public StatementRestrictions(ClientState state,
                                 StatementType type,
                                 TableMetadata table,
                                 WhereClause whereClause,
                                 VariableSpecifications boundNames,
                                 List<Ordering> orderings,
                                 boolean selectsOnlyStaticColumns,
                                 boolean allowUseOfSecondaryIndices,
                                 boolean allowFiltering,
                                 boolean forView)
    {
        this(type, table, allowFiltering);

        final IndexRegistry indexRegistry = type.allowUseOfSecondaryIndices() ? IndexRegistry.obtain(table) : null;

        // The WHERE clause root is always a conjunction. Its leaf children are processed by the
        // conjunctive merge machinery exactly as before, while OR subtrees are validated leaf by
        // leaf and kept aside in per-subtree holders, never merged.
        List<Relation> relations = new ArrayList<>();
        List<CustomIndexExpression> customExpressions = new ArrayList<>();
        List<WhereClause.OrElement> orElements = new ArrayList<>();
        for (WhereClause.ExpressionElement element : whereClause.root().children())
        {
            if (element instanceof WhereClause.RelationElement)
                relations.add(((WhereClause.RelationElement) element).relation());
            else if (element instanceof WhereClause.CustomIndexExpressionElement)
                customExpressions.add(((WhereClause.CustomIndexExpressionElement) element).expression());
            else
                orElements.add((WhereClause.OrElement) element);
        }

        if (!orElements.isEmpty() && table.isVirtual())
            throw invalidRequest("OR is not supported on virtual tables");

        // A custom index expression pins the query to its target index, which cannot execute a
        // disjunctive filter, so the combination is refused outright
        if (!orElements.isEmpty() && !customExpressions.isEmpty())
            throw invalidRequest("Custom index expressions are not supported in queries containing OR");

        /*
         * WHERE clause. For a given entity, rules are:
         *   - EQ relation conflicts with anything else (including a 2nd EQ)
         *   - Can't have more than one LT(E) relation (resp. GT(E) relation)
         *   - IN relation are restricted to row keys (for now) and conflicts with anything else (we could
         *     allow two IN for the same entity but that doesn't seem very useful)
         *   - The value_alias cannot be restricted in any way (we don't support wide rows with indexed value
         *     in CQL so far)
         *   - CONTAINS and CONTAINS_KEY cannot be used with UPDATE or DELETE
         */
        for (Relation relation : relations)
        {
            if ((relation.isContains() || relation.isContainsKey() || relation.operator().isAnalyzed())
                && (type.isUpdate() || type.isDelete()))
            {
                throw invalidRequest("Cannot use %s with %s", type, relation.operator());
            }

            if (relation.operator() == Operator.IS_NOT)
            {
                if (!forView)
                    throw new InvalidRequestException("Unsupported restriction: " + relation);

                this.notNullColumns.addAll(relation.toRestriction(table, boundNames).getColumnDefs());
            }
            else if (relation.isLIKE())
            {
                Restriction restriction = relation.toRestriction(table, boundNames);

                if (!type.allowUseOfSecondaryIndices() || !restriction.hasSupportingIndex(indexRegistry))
                    throw new InvalidRequestException(String.format("LIKE restriction is only supported on properly " +
                                                                    "indexed columns. %s is not valid.",
                                                                    relation));

                addRestriction(restriction, indexRegistry);
            }
            else if (relation.operator().isAnalyzed())
            {
                Restriction restriction = relation.toRestriction(table, boundNames);

                if (!type.allowUseOfSecondaryIndices() || !restriction.hasSupportingIndex(indexRegistry))
                    throw analyzedOperatorRequiresIndex(relation, restriction);

                addRestriction(restriction, indexRegistry);
            }
            else
            {
                checkContainsIsNotAnalyzed(relation, indexRegistry);
                addRestriction(relation.toRestriction(table, boundNames), indexRegistry);
            }
        }

        for (WhereClause.OrElement orElement : orElements)
            disjunctions.add(prepareDisjunction(orElement, boundNames, indexRegistry));

        // ORDER BY clause.
        // Some indexes can be used for ordering.
        nonPrimaryKeyRestrictions = addOrderingRestrictions(orderings, nonPrimaryKeyRestrictions);

        hasRegularColumnsRestrictions = nonPrimaryKeyRestrictions.hasRestrictionFor(ColumnMetadata.Kind.REGULAR)
                                        || disjunctionsRestrictRegularColumns();

        boolean hasQueriableClusteringColumnIndex = false;
        boolean hasQueriableIndex = false;

        if (allowUseOfSecondaryIndices)
        {
            if (!customExpressions.isEmpty())
                processCustomIndexExpressions(customExpressions, boundNames, indexRegistry);

            hasQueriableClusteringColumnIndex = clusteringColumnsRestrictions.hasSupportingIndex(indexRegistry);
            hasQueriableIndex = !filterRestrictions.getCustomIndexExpressions().isEmpty()
                    || hasQueriableClusteringColumnIndex
                    || partitionKeyRestrictions.hasSupportingIndex(indexRegistry)
                    || nonPrimaryKeyRestrictions.hasSupportingIndex(indexRegistry);
        }

        // At this point, the select statement if fully constructed, but we still have a few things to validate
        processPartitionKeyRestrictions(state, hasQueriableIndex, allowFiltering, forView);

        // Some but not all of the partition key columns have been specified;
        // hence we need turn these restrictions into a row filter.
        if (usesSecondaryIndexing || partitionKeyRestrictions.needFiltering(table))
            filterRestrictions.add(partitionKeyRestrictions);

        if (selectsOnlyStaticColumns && hasClusteringColumnsRestrictions())
        {
            // If the only updated/deleted columns are static, then we don't need clustering columns.
            // And in fact, unless it is an INSERT, we reject if clustering colums are provided as that
            // suggest something unintended. For instance, given:
            //   CREATE TABLE t (k int, v int, s int static, PRIMARY KEY (k, v))
            // it can make sense to do:
            //   INSERT INTO t(k, v, s) VALUES (0, 1, 2)
            // but both
            //   UPDATE t SET s = 3 WHERE k = 0 AND v = 1
            //   DELETE v FROM t WHERE k = 0 AND v = 1
            // sounds like you don't really understand what your are doing.
            if (type.isDelete() || type.isUpdate())
                throw invalidRequest("Invalid restrictions on clustering columns since the %s statement modifies only static columns",
                                     type);
            if (type.isSelect())
                throw invalidRequest("Cannot restrict clustering columns when selecting only static columns");
        }

        processClusteringColumnsRestrictions(hasQueriableIndex,
                                             selectsOnlyStaticColumns,
                                             forView,
                                             allowFiltering);

        // Covers indexes on the first clustering column (among others).
        if (isKeyRange && hasQueriableClusteringColumnIndex)
            usesSecondaryIndexing = true;

        if (usesSecondaryIndexing || clusteringColumnsRestrictions.needFiltering())
            filterRestrictions.add(clusteringColumnsRestrictions);

        // Even if usesSecondaryIndexing is false at this point, we'll still have to use one if
        // there is restrictions not covered by the PK.
        if (!nonPrimaryKeyRestrictions.isEmpty())
        {
            if (!type.allowNonPrimaryKeyInWhereClause())
            {
                Collection<ColumnIdentifier> nonPrimaryKeyColumns =
                        ColumnMetadata.toIdentifiers(nonPrimaryKeyRestrictions.getColumnDefs());

                throw invalidRequest("Non PRIMARY KEY columns found in where clause: %s ",
                                     Joiner.on(", ").join(nonPrimaryKeyColumns));
            }

            Optional<SingleRestriction> annRestriction = Streams.stream(nonPrimaryKeyRestrictions).filter(SingleRestriction::isANN).findFirst();
            if (annRestriction.isPresent())
            {
                // If there is an ANN restriction then it must be for a vector<float, n> column, and it must have an index
                ColumnMetadata annColumn = annRestriction.get().getFirstColumn();

                if (!annColumn.type.isVector() || !(((VectorType<?>)annColumn.type).elementType instanceof FloatType))
                    throw invalidRequest(StatementRestrictions.ANN_ONLY_SUPPORTED_ON_VECTOR_MESSAGE);
                if (indexRegistry == null || indexRegistry.listIndexes().stream().noneMatch(i -> i.dependsOn(annColumn)))
                    throw invalidRequest(StatementRestrictions.ANN_REQUIRES_INDEX_MESSAGE);
                // We do not allow ANN queries using partition key restrictions that need filtering
                if (partitionKeyRestrictions.needFiltering(table))
                    throw invalidRequest(StatementRestrictions.ANN_REQUIRES_INDEXED_FILTERING_MESSAGE);
                // We do not allow ANN query filtering using non-indexed columns
                List<ColumnMetadata> nonAnnColumns = Streams.stream(nonPrimaryKeyRestrictions)
                                                            .filter(r -> !r.isANN())
                                                            .map(Restriction::getFirstColumn)
                                                            .collect(Collectors.toList());
                Collection<ColumnMetadata> clusteringColumns = clusteringColumnsRestrictions.getColumnDefinitions();
                if (!nonAnnColumns.isEmpty() || !clusteringColumns.isEmpty())
                {
                    List<ColumnMetadata> nonIndexedColumns = Stream.concat(nonAnnColumns.stream(), clusteringColumns.stream())
                                                                   .filter(c -> indexRegistry.listIndexes().stream().noneMatch(i -> i.dependsOn(c)))
                                                                   .collect(Collectors.toList());

                    if (!nonIndexedColumns.isEmpty())
                    {
                        // restrictions on non-clustering columns, or clusterings that still need filtering, are invalid
                        if (!clusteringColumns.containsAll(nonIndexedColumns)
                                || partitionKeyRestrictions.hasUnrestrictedPartitionKeyComponents(table)
                                || clusteringColumnsRestrictions.needFiltering())
                            throw invalidRequest(StatementRestrictions.ANN_REQUIRES_INDEXED_FILTERING_MESSAGE);
                    }
                }
            }
            else
            {
                // We do not support indexed vector restrictions that are not part of an ANN ordering
                Optional<ColumnMetadata> vectorColumn = nonPrimaryKeyRestrictions.getColumnDefs()
                                                                                 .stream()
                                                                                 .filter(c -> c.type.isVector())
                                                                                 .findFirst();
                if (vectorColumn.isPresent() && indexRegistry.listIndexes().stream().anyMatch(i -> i.dependsOn(vectorColumn.get())))
                    throw invalidRequest(StatementRestrictions.VECTOR_INDEXES_ANN_ONLY_MESSAGE);
            }

            if (hasQueriableIndex)
            {
                usesSecondaryIndexing = true;
            }
            else
            {
                if (!allowFiltering && requiresAllowFilteringIfNotSpecified())
                    throw invalidRequest(allowFilteringMessage(state));
            }

            filterRestrictions.add(nonPrimaryKeyRestrictions);
        }

        // This block runs after the ones above consumed isKeyRange and usesSecondaryIndexing on
        // purpose: partition key and clustering restrictions must never enter the row filter for
        // a query whose only index interaction is its disjunctions. Correctness of the forced
        // range read rests on getPartitionKeyBounds deriving exact partition bounds (IN is
        // rejected below) and on the DataRange clustering filter applying the clustering
        // restrictions. Moving this above them would flip those add decisions.
        if (!disjunctions.isEmpty())
        {
            // Top-K plumbing consumes a single index result stream and is out of scope for OR
            if (nonPrimaryKeyRestrictions.hasAnn())
                throw invalidRequest("ANN ordering is not supported in queries containing OR");

            // The range read below derives one contiguous partition bound, which cannot
            // represent a partition key IN list
            if (partitionKeyRestrictions.hasIN())
                throw invalidRequest("IN restrictions on the partition key are not supported in queries containing OR");

            // No SASI index runs next to OR, and a root condition only a SASI index that changes values can
            // answer gives different rows when compared with the stored value
            checkRootIsNotOnlyServedByValueChangingSasi(indexRegistry);

            // A disjunct can match rows in any partition, so the query is always a range read.
            // Cut 1 does not optimize a partition restricted query with a disjunction into a
            // slice read even though it could.
            isKeyRange = true;

            // A disjunction can only use an index when every leaf of every branch is supported
            // by an index group that understands OR, otherwise it runs on the filtering path
            disjunctionsNeedFiltering = !allowUseOfSecondaryIndices || !disjunctionsAreIndexSupported(indexRegistry);

            if (disjunctionsNeedFiltering)
            {
                if (!allowFiltering && requiresAllowFilteringIfNotSpecified())
                    throw invalidRequest(allowFilteringMessage(state));
            }
            else
            {
                usesSecondaryIndexing = true;
            }
        }

        if (usesSecondaryIndexing)
            validateSecondaryIndexSelections();
    }

    public boolean requiresAllowFilteringIfNotSpecified()
    {
        if (!table.isVirtual())
            return true;

        VirtualTable tableNullable = VirtualKeyspaceRegistry.instance.getTableNullable(table.id);
        assert tableNullable != null;
        return !tableNullable.allowFilteringImplicitly();
    }

    private void addRestriction(Restriction restriction, IndexRegistry indexRegistry)
    {
        ColumnMetadata def = restriction.getFirstColumn();
        if (def.isPartitionKey())
            partitionKeyRestrictions = partitionKeyRestrictions.mergeWith(restriction);
        else if (def.isClusteringColumn())
            clusteringColumnsRestrictions = clusteringColumnsRestrictions.mergeWith(restriction, indexRegistry);
        else
            nonPrimaryKeyRestrictions = nonPrimaryKeyRestrictions.addRestriction((SingleRestriction) restriction);
    }

    /**
     * Validates one OR subtree of the WHERE clause and converts its relation leaves into
     * restrictions, one per leaf. Relations on one column in one AND group are also merged by the
     * rules the root uses.
     */
    private DisjunctionHolder prepareDisjunction(WhereClause.OrElement element,
                                                 VariableSpecifications boundNames,
                                                 IndexRegistry indexRegistry)
    {
        Map<WhereClause.RelationElement, SingleRestriction> leafRestrictions = new IdentityHashMap<>();
        DisjunctionHolder holder = new DisjunctionHolder(element, leafRestrictions);
        prepareDisjunctionLeaves(element, boundNames, indexRegistry, holder);
        return holder;
    }

    private void prepareDisjunctionLeaves(WhereClause.ContainerElement container,
                                          VariableSpecifications boundNames,
                                          IndexRegistry indexRegistry,
                                          DisjunctionHolder holder)
    {
        for (WhereClause.ExpressionElement child : container.children())
        {
            if (child instanceof WhereClause.ContainerElement)
                prepareDisjunctionLeaves((WhereClause.ContainerElement) child, boundNames, indexRegistry, holder);
            else if (child instanceof WhereClause.CustomIndexExpressionElement)
                throw invalidRequest("Custom index expressions are not supported within OR expressions");
            else
            {
                WhereClause.RelationElement leaf = (WhereClause.RelationElement) child;
                holder.leafRestrictions.put(leaf, prepareDisjunctionLeaf(leaf.relation(), boundNames, indexRegistry));
            }
        }

        // The direct leaves of an OR are separate branches and never merge
        if (!(container instanceof WhereClause.AndElement))
            return;

        // Direct relations of an AND group on one column merge as at the root: the same mergeWith call
        // in the order written, so a pair the root refuses is refused with the same text. Every leaf keeps
        // its own restriction, so index support is judged per leaf.
        Map<ColumnMetadata, List<WhereClause.RelationElement>> leavesByColumn = new LinkedHashMap<>();
        for (WhereClause.ExpressionElement child : container.children())
        {
            if (child instanceof WhereClause.RelationElement)
                leavesByColumn.computeIfAbsent(holder.leafRestrictions.get(child).getFirstColumn(), column -> new ArrayList<>())
                              .add((WhereClause.RelationElement) child);
        }

        for (Map.Entry<ColumnMetadata, List<WhereClause.RelationElement>> entry : leavesByColumn.entrySet())
        {
            List<WhereClause.RelationElement> leaves = entry.getValue();
            if (leaves.size() < 2)
                continue;

            SingleRestriction merged = holder.leafRestrictions.get(leaves.get(0));
            try
            {
                for (WhereClause.RelationElement leaf : leaves.subList(1, leaves.size()))
                    merged = merged.mergeWith(holder.leafRestrictions.get(leaf));
            }
            catch (InvalidRequestException e)
            {
                Tracing.trace("OR group refused for several relations on column {}", entry.getKey().name);
                throw e;
            }
            holder.mergedRestrictions.put(leaves.get(0), merged);
            holder.absorbedLeaves.addAll(leaves.subList(1, leaves.size()));
        }
    }

    /**
     * Refuses CONTAINS and CONTAINS KEY when every index that serves the relation's target has an
     * index_analyzer. Such an index holds words, not whole elements or keys, so it cannot answer these
     * operators. When any other index serves the target, that index answers them and nothing is refused.
     */
    private void checkContainsIsNotAnalyzed(Relation relation, IndexRegistry indexRegistry)
    {
        if (indexRegistry == null || !(relation.isContains() || relation.isContainsKey()))
            return;

        ColumnMetadata column = table.getExistingColumn(((SingleColumnRelation) relation).getEntity());
        Operator operator = relation.operator();
        boolean servedByAnalyzedIndex = false;
        for (Index index : indexRegistry.listIndexes())
        {
            if (index instanceof StorageAttachedIndex && ((StorageAttachedIndex) index).hasLuceneAnalyzer())
            {
                StorageAttachedIndex analyzed = (StorageAttachedIndex) index;
                if (analyzed.dependsOn(column) && analyzed.termType().supports(operator))
                    servedByAnalyzedIndex = true;
            }
            else if (index.supportsExpression(column, operator))
            {
                return;
            }
        }

        if (servedByAnalyzedIndex)
            throw invalidRequest(relation.isContains() ? ANALYZED_CONTAINS_MESSAGE : ANALYZED_CONTAINS_KEY_MESSAGE, column.name);
    }

    /**
     * The error for an analyzed operator no index can serve. MATCH KEY and PHRASE KEY name the index
     * to create, because an analyzed index on the map's values does not serve them.
     */
    private static InvalidRequestException analyzedOperatorRequiresIndex(Relation relation, Restriction restriction)
    {
        if (relation.operator().targetsMapKeys())
            return invalidRequest(MAP_KEYS_OPERATOR_REQUIRES_INDEX_MESSAGE, relation.operator(), restriction.getFirstColumn().name);

        return invalidRequest(ANALYZED_OPERATOR_REQUIRES_INDEX_MESSAGE, relation.operator(), relation);
    }

    private SingleRestriction prepareDisjunctionLeaf(Relation relation,
                                                     VariableSpecifications boundNames,
                                                     IndexRegistry indexRegistry)
    {
        if (relation.onToken())
            throw invalidRequest("Restrictions on partition key columns are not supported within OR expressions");

        if (relation.isIN())
            throw invalidRequest("IN restrictions are not supported within OR expressions");

        if (relation.isLIKE())
            throw invalidRequest("LIKE restrictions are not supported within OR expressions");

        if (relation.operator() == Operator.IS_NOT)
            throw invalidRequest("IS NOT NULL restrictions are not supported within OR expressions");

        if (relation.operator() == Operator.ANN)
            throw invalidRequest("ANN restrictions are not supported within OR expressions");

        Restriction restriction = relation.toRestriction(table, boundNames);

        for (ColumnMetadata column : restriction.getColumnDefs())
        {
            if (column.isPartitionKey())
                throw invalidRequest("Restrictions on partition key columns are not supported within OR expressions");

            if (column.isClusteringColumn())
                throw invalidRequest("Restrictions on clustering columns are not supported within OR expressions");
        }

        checkContainsIsNotAnalyzed(relation, indexRegistry);

        if (relation.operator().isAnalyzed()
            && (!type.allowUseOfSecondaryIndices() || !restriction.hasSupportingIndex(indexRegistry)))
        {
            throw analyzedOperatorRequiresIndex(relation, restriction);
        }

        return (SingleRestriction) restriction;
    }

    /**
     * Refuses a root condition on a regular or static column when every index group that serves it is a SASI
     * index whose analyzer changes values. Such a condition sits next to OR, where no SASI index runs.
     */
    private void checkRootIsNotOnlyServedByValueChangingSasi(IndexRegistry indexRegistry)
    {
        if (indexRegistry == null)
            return;

        for (SingleRestriction restriction : nonPrimaryKeyRestrictions)
        {
            boolean servedByValueChangingSasi = false;
            boolean servedOtherwise = false;
            for (Index.Group group : indexRegistry.listIndexGroups())
            {
                if (restriction.needsFiltering(group))
                    continue;

                if (Iterables.all(group.getIndexes(), StorageAttachedIndex::sasiIndexChangesValues))
                    servedByValueChangingSasi = true;
                else
                    servedOtherwise = true;
            }

            if (servedByValueChangingSasi && !servedOtherwise)
            {
                ColumnMetadata column = restriction.getFirstColumn();
                Tracing.trace("OR query refused: column {} is only answered by a SASI index that changes values", column.name);
                throw invalidRequest(VALUE_CHANGING_SASI_NEXT_TO_OR_MESSAGE, column.name);
            }
        }
    }

    /**
     * @return true when the query has disjunctions and some root condition on a regular or static column is not
     * served by any index group that can execute disjunctions. The query then filters, as a query without OR
     * does when no one index group serves every condition. The caller returns earlier when the disjunctions
     * themselves need filtering.
     */
    private boolean rootNeedsFilteringNextToDisjunctions(IndexRegistry indexRegistry)
    {
        if (disjunctions.isEmpty())
            return false;

        for (SingleRestriction restriction : nonPrimaryKeyRestrictions)
        {
            boolean served = false;
            for (Index.Group group : indexRegistry.listIndexGroups())
            {
                if (group.supportsDisjunction() && !restriction.needsFiltering(group))
                {
                    served = true;
                    break;
                }
            }
            if (!served)
            {
                Tracing.trace("OR query needs ALLOW FILTERING: column {} is not served by the SAI index group", restriction.getFirstColumn().name);
                return true;
            }
        }
        return false;
    }

    /**
     * A copy of the root expressions of the filter with a LIKE without a wildcard as =. That is what it means to a
     * SASI index that keeps values as they are, and next to OR no SASI index runs. On the index path a LIKE on a
     * column with an SAI index stays a LIKE, since that index answers = with its own meaning. The SAI query
     * plan checks it as a raw = on the candidate rows instead.
     */
    private RowFilter withLikeMatchesAsEquality(RowFilter filter, boolean needsReconciliation, IndexRegistry indexRegistry)
    {
        boolean indexPath = !disjunctionsNeedFiltering && disjunctionsAreIndexSupported(indexRegistry);
        RowFilter rewritten = RowFilter.create(needsReconciliation);
        for (RowFilter.Expression expression : filter.root().expressions())
        {
            if (expression.operator() == Operator.LIKE_MATCHES && !(indexPath && hasStorageAttachedIndex(indexRegistry, expression.column())))
                rewritten.add(expression.column(), Operator.EQ, expression.getIndexValue());
            else
                rewritten.root().add(expression);
        }
        return rewritten;
    }

    private static boolean hasStorageAttachedIndex(IndexRegistry indexRegistry, ColumnMetadata column)
    {
        for (Index index : indexRegistry.listIndexes())
            if (index instanceof StorageAttachedIndex && index.dependsOn(column))
                return true;
        return false;
    }

    /**
     * @return true if every leaf of every disjunction is supported by an index whose group can
     * execute disjunctions
     */
    private boolean disjunctionsAreIndexSupported(IndexRegistry indexRegistry)
    {
        if (indexRegistry == null)
            return false;

        for (DisjunctionHolder holder : disjunctions)
        {
            for (SingleRestriction restriction : holder.leafRestrictions.values())
            {
                boolean supported = false;
                for (Index.Group group : indexRegistry.listIndexGroups())
                {
                    if (group.supportsDisjunction() && !restriction.needsFiltering(group))
                    {
                        supported = true;
                        break;
                    }
                }
                if (!supported)
                    return false;
            }
        }
        return true;
    }

    private boolean disjunctionsRestrictRegularColumns()
    {
        for (DisjunctionHolder holder : disjunctions)
            for (SingleRestriction restriction : holder.leafRestrictions.values())
                for (ColumnMetadata column : restriction.getColumnDefs())
                    if (column.isRegular())
                        return true;

        return false;
    }

    public void addFunctionsTo(List<Function> functions)
    {
        partitionKeyRestrictions.addFunctionsTo(functions);
        clusteringColumnsRestrictions.addFunctionsTo(functions);
        nonPrimaryKeyRestrictions.addFunctionsTo(functions);

        for (DisjunctionHolder holder : disjunctions)
            for (SingleRestriction restriction : holder.leafRestrictions.values())
                restriction.addFunctionsTo(functions);
    }

    // may be used by QueryHandler implementations
    public IndexRestrictions getIndexRestrictions()
    {
        return filterRestrictions;
    }

    /**
     * Returns the non-PK column that are restricted.  If includeNotNullRestrictions is true, columns that are restricted
     * by an IS NOT NULL restriction will be included, otherwise they will not be included (unless another restriction
     * applies to them).
     */
    public Set<ColumnMetadata> nonPKRestrictedColumns(boolean includeNotNullRestrictions)
    {
        Set<ColumnMetadata> columns = new HashSet<>();
        for (Restrictions r : filterRestrictions.getRestrictions())
        {
            for (ColumnMetadata def : r.getColumnDefs())
                if (!def.isPrimaryKeyColumn())
                    columns.add(def);
        }

        for (DisjunctionHolder holder : disjunctions)
            for (SingleRestriction restriction : holder.leafRestrictions.values())
                for (ColumnMetadata def : restriction.getColumnDefs())
                    if (!def.isPrimaryKeyColumn())
                        columns.add(def);

        if (includeNotNullRestrictions)
        {
            for (ColumnMetadata def : notNullColumns)
            {
                if (!def.isPrimaryKeyColumn())
                    columns.add(def);
            }
        }

        return columns;
    }

    /**
     * @return the set of columns that have an IS NOT NULL restriction on them
     */
    public Set<ColumnMetadata> notNullColumns()
    {
        return notNullColumns;
    }

    /**
     * @return true if column is restricted by some restriction, false otherwise
     */
    public boolean isRestricted(ColumnMetadata column)
    {
        if (notNullColumns.contains(column))
            return true;

        return getRestrictions(column.kind).getColumnDefs().contains(column);
    }

    /**
     * Checks if the restrictions on the partition key has IN restrictions.
     *
     * @return <code>true</code> the restrictions on the partition key has an IN restriction, <code>false</code>
     * otherwise.
     */
    public boolean keyIsInRelation()
    {
        return partitionKeyRestrictions.hasIN();
    }

    /**
     * Checks if the query request a range of partition keys.
     *
     * @return <code>true</code> if the query request a range of partition keys, <code>false</code> otherwise.
     */
    public boolean isKeyRange()
    {
        return this.isKeyRange;
    }

    /**
     * Checks if the specified column is restricted by an EQ restriction.
     *
     * @param columnDef the column definition
     * @return <code>true</code> if the specified column is restricted by an EQ restiction, <code>false</code>
     * otherwise.
     */
    public boolean isColumnRestrictedByEq(ColumnMetadata columnDef)
    {
        Set<Restriction> restrictions = getRestrictions(columnDef.kind).getRestrictions(columnDef);
        return restrictions.stream()
                           .filter(SingleRestriction.class::isInstance)
                           .anyMatch(p -> ((SingleRestriction) p).isEQ());
    }

    /**
     * This method determines whether a specified column is restricted on equality or something equivalent, like IN.
     * It can be used in conjunction with the columns selected by a query to determine which of those columns is
     * already bound by the client (and from its perspective, not retrieved by the database).
     *
     * @param column a column from the same table these restrictions are against
     *
     * @return <code>true</code> if the given column is restricted on equality
     */
    public boolean isEqualityRestricted(ColumnMetadata column)
    {
        if (column.kind == ColumnMetadata.Kind.PARTITION_KEY)
        {
            if (partitionKeyRestrictions.hasOnlyEqualityRestrictions())
                for (ColumnMetadata restricted : partitionKeyRestrictions.getColumnDefinitions())
                    if (restricted.name.equals(column.name))
                        return true;
        }
        else if (column.kind == ColumnMetadata.Kind.CLUSTERING)
        {
            if (hasClusteringColumnsRestrictions())
            {
                for (SingleRestriction restriction : clusteringColumnsRestrictions.getRestrictionSet())
                {
                    if (restriction.isEqualityBased())
                    {
                        if (restriction.isMultiColumn())
                        {
                            for (ColumnMetadata restricted : restriction.getColumnDefs())
                                if (restricted.name.equals(column.name))
                                    return true;
                        }
                        else if (restriction.getFirstColumn().name.equals(column.name))
                            return true;
                    }
                }
            }
        }
        else if (hasNonPrimaryKeyRestrictions())
        {
            for (SingleRestriction restriction : nonPrimaryKeyRestrictions)
                if (restriction.getFirstColumn().name.equals(column.name) && restriction.isEqualityBased())
                    return true;
        }

        return false;
    }

    public boolean isTopK()
    {
        return nonPrimaryKeyRestrictions.hasAnn();
    }
    /**
     * Returns the <code>Restrictions</code> for the specified type of columns.
     *
     * @param kind the column type
     * @return the <code>Restrictions</code> for the specified type of columns
     */
    private Restrictions getRestrictions(ColumnMetadata.Kind kind)
    {
        switch (kind)
        {
            case PARTITION_KEY: return partitionKeyRestrictions;
            case CLUSTERING: return clusteringColumnsRestrictions;
            default: return nonPrimaryKeyRestrictions;
        }
    }

    /**
     * Checks if the secondary index need to be queried.
     *
     * @return <code>true</code> if the secondary index need to be queried, <code>false</code> otherwise.
     */
    public boolean usesSecondaryIndexing()
    {
        return this.usesSecondaryIndexing;
    }

    /**
     * This is a hack to push ordering down to indexes.
     * Indexes are selected based on RowFilter only, so we need to turn orderings into restrictions
     * so they end up in the row filter.
     *
     * @param orderings orderings from the select statement
     * @return the {@link RestrictionSet} with the added orderings
     */
    private RestrictionSet addOrderingRestrictions(List<Ordering> orderings, RestrictionSet restrictionSet)
    {
        List<Ordering> annOrderings = orderings.stream().filter(o -> o.expression.hasNonClusteredOrdering()).collect(Collectors.toList());

        if (annOrderings.size() > 1)
            throw new InvalidRequestException("Cannot specify more than one ANN ordering");
        else if (annOrderings.size() == 1)
        {
            if (orderings.size() > 1)
                throw new InvalidRequestException("ANN ordering does not support any other ordering");
            Ordering annOrdering = annOrderings.get(0);
            if (annOrdering.direction != Ordering.Direction.ASC)
                throw new InvalidRequestException("Descending ANN ordering is not supported");
            SingleRestriction restriction = annOrdering.expression.toRestriction();
            return restrictionSet.addRestriction(restriction);
        }
        return restrictionSet;
    }

    private static Iterable<Restriction> allColumnRestrictions(ClusteringColumnRestrictions clusteringColumnsRestrictions, RestrictionSet nonPrimaryKeyRestrictions)
    {
        return Iterables.concat(clusteringColumnsRestrictions.getRestrictionSet(), nonPrimaryKeyRestrictions);
    }

    private void processPartitionKeyRestrictions(ClientState state, boolean hasQueriableIndex, boolean allowFiltering, boolean forView)
    {
        if (!type.allowPartitionKeyRanges())
        {
            checkFalse(partitionKeyRestrictions.isOnToken(),
                       "The token function cannot be used in WHERE clauses for %s statements", type);

            if (partitionKeyRestrictions.hasUnrestrictedPartitionKeyComponents(table))
                throw invalidRequest("Some partition key parts are missing: %s",
                                     Joiner.on(", ").join(getPartitionKeyUnrestrictedComponents()));

            // slice query
            checkFalse(partitionKeyRestrictions.hasSlice(),
                    "Only EQ and IN relation are supported on the partition key (unless you use the token() function)"
                            + " for %s statements", type);
        }
        else
        {
            // If there are no partition restrictions or there's only token restriction, we have to set a key range
            if (partitionKeyRestrictions.isOnToken())
                isKeyRange = true;

            if (partitionKeyRestrictions.isEmpty() && partitionKeyRestrictions.hasUnrestrictedPartitionKeyComponents(table))
            {
                isKeyRange = true;
                usesSecondaryIndexing = hasQueriableIndex;
            }

            // If there is a queriable index, no special condition is required on the other restrictions.
            // But we still need to know 2 things:
            // - If we don't have a queriable index, is the query ok
            // - Is it queriable without 2ndary index, which is always more efficient
            // If a component of the partition key is restricted by a relation, all preceding
            // components must have a EQ. Only the last partition key component can be in IN relation.
            if (partitionKeyRestrictions.needFiltering(table))
            {
                if (!allowFiltering && !forView && !hasQueriableIndex && requiresAllowFilteringIfNotSpecified())
                    throw new InvalidRequestException(allowFilteringMessage(state));

                isKeyRange = true;
                usesSecondaryIndexing = hasQueriableIndex;
            }
        }
    }

    public boolean hasPartitionKeyRestrictions()
    {
        return !partitionKeyRestrictions.isEmpty();
    }

    /**
     * Checks if the restrictions contain any non-primary key restrictions
     * @return <code>true</code> if the restrictions contain any non-primary key restrictions, <code>false</code> otherwise.
     */
    public boolean hasNonPrimaryKeyRestrictions()
    {
        return !nonPrimaryKeyRestrictions.isEmpty();
    }

    /**
     * Returns the partition key components that are not restricted.
     * @return the partition key components that are not restricted.
     */
    private Collection<ColumnIdentifier> getPartitionKeyUnrestrictedComponents()
    {
        List<ColumnMetadata> list = new ArrayList<>(table.partitionKeyColumns());
        list.removeAll(partitionKeyRestrictions.getColumnDefs());
        return ColumnMetadata.toIdentifiers(list);
    }

    /**
     * Checks if the restrictions on the partition key are token restrictions.
     *
     * @return <code>true</code> if the restrictions on the partition key are token restrictions,
     * <code>false</code> otherwise.
     */
    public boolean isPartitionKeyRestrictionsOnToken()
    {
        return partitionKeyRestrictions.isOnToken();
    }

    /**
     * Checks if restrictions on the clustering key have IN restrictions.
     *
     * @return <code>true</code> if the restrictions on the clustering key have IN restrictions,
     * <code>false</code> otherwise.
     */
    public boolean clusteringKeyRestrictionsHasIN()
    {
        return clusteringColumnsRestrictions.hasIN();
    }

    /**
     * Processes the clustering column restrictions.
     *
     * @param hasQueriableIndex <code>true</code> if some of the queried data are indexed, <code>false</code> otherwise
     * @param selectsOnlyStaticColumns <code>true</code> if the selected or modified columns are all statics,
     * <code>false</code> otherwise.
     */
    private void processClusteringColumnsRestrictions(boolean hasQueriableIndex,
                                                      boolean selectsOnlyStaticColumns,
                                                      boolean forView,
                                                      boolean allowFiltering)
    {
        checkFalse(!type.allowClusteringColumnSlices() && clusteringColumnsRestrictions.hasSlice(),
                   "Slice restrictions are not supported on the clustering columns in %s statements", type);

        if (!type.allowClusteringColumnSlices()
            && (!table.isCompactTable() || (table.isCompactTable() && !hasClusteringColumnsRestrictions())))
        {
            if (!selectsOnlyStaticColumns && hasUnrestrictedClusteringColumns())
                throw invalidRequest("Some clustering keys are missing: %s",
                                     Joiner.on(", ").join(getUnrestrictedClusteringColumns()));
        }
        else
        {
            checkFalse(clusteringColumnsRestrictions.hasContains() && !hasQueriableIndex && !allowFiltering,
                       "Clustering columns can only be restricted with CONTAINS with a secondary index or filtering");

            if (hasClusteringColumnsRestrictions() && clusteringColumnsRestrictions.needFiltering())
            {
                if (hasQueriableIndex || forView)
                {
                    usesSecondaryIndexing = true;
                }
                else if (!allowFiltering)
                {
                    List<ColumnMetadata> clusteringColumns = table.clusteringColumns();
                    List<ColumnMetadata> restrictedColumns = new LinkedList<>(clusteringColumnsRestrictions.getColumnDefs());

                    for (int i = 0, m = restrictedColumns.size(); i < m; i++)
                    {
                        ColumnMetadata clusteringColumn = clusteringColumns.get(i);
                        ColumnMetadata restrictedColumn = restrictedColumns.get(i);

                        if (!clusteringColumn.equals(restrictedColumn))
                        {
                            throw invalidRequest("PRIMARY KEY column \"%s\" cannot be restricted as preceding column \"%s\" is not restricted",
                                                 restrictedColumn.name,
                                                 clusteringColumn.name);
                        }
                    }
                }
            }

        }

    }

    /**
     * Returns the clustering columns that are not restricted.
     * @return the clustering columns that are not restricted.
     */
    private Collection<ColumnIdentifier> getUnrestrictedClusteringColumns()
    {
        List<ColumnMetadata> missingClusteringColumns = new ArrayList<>(table.clusteringColumns());
        missingClusteringColumns.removeAll(new LinkedList<>(clusteringColumnsRestrictions.getColumnDefs()));
        return ColumnMetadata.toIdentifiers(missingClusteringColumns);
    }

    /**
     * Checks if some clustering columns are not restricted.
     * @return <code>true</code> if some clustering columns are not restricted, <code>false</code> otherwise.
     */
    private boolean hasUnrestrictedClusteringColumns()
    {
        return table.clusteringColumns().size() != clusteringColumnsRestrictions.size();
    }

    private void processCustomIndexExpressions(List<CustomIndexExpression> expressions,
                                               VariableSpecifications boundNames,
                                               IndexRegistry indexRegistry)
    {
        if (expressions.size() > 1)
            throw new InvalidRequestException(IndexRestrictions.MULTIPLE_EXPRESSIONS);

        CustomIndexExpression expression = expressions.get(0);

        QualifiedName name = expression.targetIndex;

        if (name.hasKeyspace() && !name.getKeyspace().equals(table.keyspace))
            throw IndexRestrictions.invalidIndex(expression.targetIndex, table);

        if (!table.indexes.has(expression.targetIndex.getName()))
            throw IndexRestrictions.indexNotFound(expression.targetIndex, table);

        Index index = indexRegistry.getIndex(table.indexes.get(expression.targetIndex.getName()).get());
        if (!index.getIndexMetadata().isCustom())
            throw IndexRestrictions.nonCustomIndexInExpression(expression.targetIndex);

        AbstractType<?> expressionType = index.customExpressionValueType();
        if (expressionType == null)
            throw IndexRestrictions.customExpressionNotSupported(expression.targetIndex);

        expression.prepareValue(table, expressionType, boundNames);

        filterRestrictions.add(expression);
    }

    public RowFilter getRowFilter(IndexRegistry indexRegistry, QueryOptions options)
    {
        if (filterRestrictions.isEmpty() && disjunctions.isEmpty())
            return RowFilter.none();

        // If there is only one replica, we don't need reconciliation at any consistency level.
        boolean needsReconciliation = !table.isVirtual()
                                      && options.getConsistency().needsReconciliation()
                                      && Keyspace.open(table.keyspace).getReplicationStrategy().getReplicationFactor().allReplicas > 1;

        RowFilter filter = RowFilter.create(needsReconciliation);
        for (Restrictions restrictions : filterRestrictions.getRestrictions())
            restrictions.addToRowFilter(filter, indexRegistry, options);

        for (CustomIndexExpression expression : filterRestrictions.getCustomIndexExpressions())
            expression.addToRowFilter(filter, table, options);

        // Only a root LIKE without a wildcard is rewritten, a LIKE inside OR is refused when prepared
        if (!disjunctions.isEmpty())
            filter = withLikeMatchesAsEquality(filter, needsReconciliation, indexRegistry);

        // Each disjunction becomes an OR child of the root. This is a structural mapping of the
        // WHERE clause subtree, one AST node to one filter node, with no operator rewriting.
        for (DisjunctionHolder holder : disjunctions)
        {
            filter.root().addChild(toFilterElement(holder.element, holder, indexRegistry, options));
            if (Tracing.isTracing())
                traceMergedLeaves(holder.element, holder);
        }

        return filter;
    }

    /**
     * Traces each column whose relations in one AND group of the disjunction were merged.
     */
    private static void traceMergedLeaves(WhereClause.ContainerElement container, DisjunctionHolder holder)
    {
        for (WhereClause.ExpressionElement child : container.children())
        {
            if (child instanceof WhereClause.ContainerElement)
            {
                traceMergedLeaves((WhereClause.ContainerElement) child, holder);
                continue;
            }

            SingleRestriction merged = holder.mergedRestrictions.get(child);
            if (merged == null)
                continue;

            int relations = 0;
            for (WhereClause.ExpressionElement sibling : container.children())
            {
                if (sibling == child
                    || (holder.absorbedLeaves.contains(sibling) && holder.leafRestrictions.get(sibling).getFirstColumn().equals(merged.getFirstColumn())))
                    relations++;
            }
            Tracing.trace("OR group merged {} relations on column {}", relations, merged.getFirstColumn().name);
        }
    }

    private RowFilter.FilterElement toFilterElement(WhereClause.ContainerElement container,
                                                    DisjunctionHolder holder,
                                                    IndexRegistry indexRegistry,
                                                    QueryOptions options)
    {
        boolean isDisjunction = container instanceof WhereClause.OrElement;
        RowFilter.FilterElement node = new RowFilter.FilterElement(isDisjunction, new ArrayList<>(), new ArrayList<>());
        for (WhereClause.ExpressionElement child : container.children())
        {
            if (child instanceof WhereClause.ContainerElement)
            {
                node.addChild(toFilterElement((WhereClause.ContainerElement) child, holder, indexRegistry, options));
                continue;
            }

            // A relation merged into the first relation on its column is emitted with it, at its place
            if (holder.absorbedLeaves.contains(child))
                continue;

            SingleRestriction restriction = holder.mergedRestrictions.getOrDefault(child, holder.leafRestrictions.get(child));
            RowFilter scratch = RowFilter.create(false);
            restriction.addToRowFilter(scratch, indexRegistry, options);
            List<RowFilter.Expression> expressions = scratch.root().expressions();

            // A leaf producing several expressions, like a two bound slice or a multi target
            // CONTAINS, keeps them conjoined in their own AND branch node under a disjunction
            if (isDisjunction && expressions.size() > 1)
            {
                RowFilter.FilterElement branch = node.addAndChild();
                for (RowFilter.Expression expression : expressions)
                    branch.add(expression);
            }
            else
            {
                for (RowFilter.Expression expression : expressions)
                    node.add(expression);
            }
        }
        return node;
    }

    /**
     * Returns the partition keys for which the data is requested.
     *
     * @param options the query options
     * @param state the client state
     * @return the partition keys for which the data is requested.
     */
    public List<ByteBuffer> getPartitionKeys(final QueryOptions options, ClientState state)
    {
        return partitionKeyRestrictions.values(options, state);
    }

    /**
     * Returns the specified bound of the partition key.
     *
     * @param b the boundary type
     * @param options the query options
     * @return the specified bound of the partition key
     */
    private ByteBuffer getPartitionKeyBound(Bound b, QueryOptions options)
    {
        // We deal with IN queries for keys in other places, so we know buildBound will return only one result
        return partitionKeyRestrictions.bounds(b, options).get(0);
    }

    /**
     * Returns the partition key bounds.
     *
     * @param options the query options
     * @return the partition key bounds
     */
    public AbstractBounds<PartitionPosition> getPartitionKeyBounds(QueryOptions options)
    {
        IPartitioner p = table.partitioner;

        if (partitionKeyRestrictions.isOnToken())
        {
            return getPartitionKeyBoundsForTokenRestrictions(p, options);
        }

        return getPartitionKeyBounds(p, options);
    }

    private AbstractBounds<PartitionPosition> getPartitionKeyBounds(IPartitioner p,
                                                                    QueryOptions options)
    {
        // Deal with unrestricted partition key components (special-casing is required to deal with 2i queries on the
        // first component of a composite partition key) queries that filter on the partition key.
        if (partitionKeyRestrictions.needFiltering(table))
            return new Range<>(p.getMinimumToken().minKeyBound(), p.getMinimumToken().maxKeyBound());

        ByteBuffer startKeyBytes = getPartitionKeyBound(Bound.START, options);
        ByteBuffer finishKeyBytes = getPartitionKeyBound(Bound.END, options);

        PartitionPosition startKey = PartitionPosition.ForKey.get(startKeyBytes, p);
        PartitionPosition finishKey = PartitionPosition.ForKey.get(finishKeyBytes, p);

        if (startKey.compareTo(finishKey) > 0 && !finishKey.isMinimum())
            return null;

        if (partitionKeyRestrictions.isInclusive(Bound.START))
        {
            return partitionKeyRestrictions.isInclusive(Bound.END)
                    ? new Bounds<>(startKey, finishKey)
                    : new IncludingExcludingBounds<>(startKey, finishKey);
        }

        return partitionKeyRestrictions.isInclusive(Bound.END)
                ? new Range<>(startKey, finishKey)
                : new ExcludingBounds<>(startKey, finishKey);
    }

    private AbstractBounds<PartitionPosition> getPartitionKeyBoundsForTokenRestrictions(IPartitioner p,
                                                                                        QueryOptions options)
    {
        Token startToken = getTokenBound(Bound.START, options, p);
        Token endToken = getTokenBound(Bound.END, options, p);

        boolean includeStart = partitionKeyRestrictions.isInclusive(Bound.START);
        boolean includeEnd = partitionKeyRestrictions.isInclusive(Bound.END);

        /*
         * If we ask SP.getRangeSlice() for (token(200), token(200)], it will happily return the whole ring.
         * However, wrapping range doesn't really make sense for CQL, and we want to return an empty result in that
         * case (CASSANDRA-5573). So special case to create a range that is guaranteed to be empty.
         *
         * In practice, we want to return an empty result set if either startToken > endToken, or both are equal but
         * one of the bound is excluded (since [a, a] can contains something, but not (a, a], [a, a) or (a, a)).
         * Note though that in the case where startToken or endToken is the minimum token, then this special case
         * rule should not apply.
         */
        int cmp = startToken.compareTo(endToken);
        if (!startToken.isMinimum() && !endToken.isMinimum()
                && (cmp > 0 || (cmp == 0 && (!includeStart || !includeEnd))))
            return null;

        PartitionPosition start = includeStart ? startToken.minKeyBound() : startToken.maxKeyBound();
        PartitionPosition end = includeEnd ? endToken.maxKeyBound() : endToken.minKeyBound();

        return new Range<>(start, end);
    }

    private Token getTokenBound(Bound b, QueryOptions options, IPartitioner p)
    {
        if (!partitionKeyRestrictions.hasBound(b))
            return p.getMinimumToken();

        ByteBuffer value = partitionKeyRestrictions.bounds(b, options).get(0);
        checkNotNull(value, "Invalid null token value");
        return p.getTokenFactory().fromByteArray(value);
    }

    /**
     * Checks if the query has some restrictions on the clustering columns.
     *
     * @return <code>true</code> if the query has some restrictions on the clustering columns,
     * <code>false</code> otherwise.
     */
    public boolean hasClusteringColumnsRestrictions()
    {
        return !clusteringColumnsRestrictions.isEmpty();
    }

    /**
     * Returns the requested clustering columns.
     *
     * @param options the query options
     * @param state the client state
     * @return the requested clustering columns
     */
    public NavigableSet<Clustering<?>> getClusteringColumns(QueryOptions options, ClientState state)
    {
        // If this is a names command and the table is a static compact one, then as far as CQL is concerned we have
        // only a single row which internally correspond to the static parts. In which case we want to return an empty
        // set (since that's what ClusteringIndexNamesFilter expects).
        if (table.isStaticCompactTable())
            return BTreeSet.empty(table.comparator);

        return clusteringColumnsRestrictions.valuesAsClustering(options, state);
    }

    /**
     * Returns the bounds (start or end) of the clustering columns.
     *
     * @param b the bound type
     * @param options the query options
     * @return the bounds (start or end) of the clustering columns
     */
    public NavigableSet<ClusteringBound<?>> getClusteringColumnsBounds(Bound b, QueryOptions options)
    {
        return clusteringColumnsRestrictions.boundsAsClustering(b, options);
    }

    /**
     * Checks if the query returns a range of columns.
     *
     * @return <code>true</code> if the query returns a range of columns, <code>false</code> otherwise.
     */
    public boolean isColumnRange()
    {
        int numberOfClusteringColumns = table.clusteringColumns().size();
        if (table.isStaticCompactTable())
        {
            // For static compact tables we want to ignore the fake clustering column (note that if we weren't special casing,
            // this would mean a 'SELECT *' on a static compact table would query whole partitions, even though we'll only return
            // the static part as far as CQL is concerned. This is thus mostly an optimization to use the query-by-name path).
            numberOfClusteringColumns = 0;
        }

        // it is a range query if it has at least one the column alias for which no relation is defined or is not EQ or IN.
        return clusteringColumnsRestrictions.size() < numberOfClusteringColumns
            || !clusteringColumnsRestrictions.hasOnlyEqualityRestrictions();
    }

    /**
     * Checks if the query need to use filtering.
     * @return <code>true</code> if the query need to use filtering, <code>false</code> otherwise.
     */
    public boolean needFiltering(TableMetadata table)
    {
        if (disjunctionsNeedFiltering)
            return true;

        IndexRegistry indexRegistry = IndexRegistry.obtain(table);
        if (rootNeedsFilteringNextToDisjunctions(indexRegistry))
            return true;

        if (filterRestrictions.needsFiltering(indexRegistry))
            return true;

        int numberOfRestrictions = filterRestrictions.getCustomIndexExpressions().size();
        for (Restrictions restrictions : filterRestrictions.getRestrictions())
            numberOfRestrictions += restrictions.size();

        return numberOfRestrictions == 0 && !clusteringColumnsRestrictions.isEmpty();
    }

    private void validateSecondaryIndexSelections()
    {
        checkFalse(keyIsInRelation(),
                   "Select on indexed columns and with IN clause for the PRIMARY KEY are not supported");
    }

    /**
     * Checks that all the primary key columns (partition key and clustering columns) are restricted by an equality
     * relation ('=' or 'IN').
     *
     * @return <code>true</code> if all the primary key columns are restricted by an equality relation.
     */
    public boolean hasAllPKColumnsRestrictedByEqualities()
    {
        return !isPartitionKeyRestrictionsOnToken()
                && !partitionKeyRestrictions.hasUnrestrictedPartitionKeyComponents(table)
                && (partitionKeyRestrictions.hasOnlyEqualityRestrictions())
                && !hasUnrestrictedClusteringColumns()
                && (clusteringColumnsRestrictions.hasOnlyEqualityRestrictions());
    }

    /**
     * Checks if one of the restrictions applies to a regular column.
     * @return {@code true} if one of the restrictions applies to a regular column, {@code false} otherwise.
     */
    public boolean hasRegularColumnsRestrictions()
    {
        return hasRegularColumnsRestrictions;
    }

    /**
     * Checks if the query is a full partitions selection.
     * @return {@code true} if the query is a full partitions selection, {@code false} otherwise.
     */
    private boolean queriesFullPartitions()
    {
        return !hasClusteringColumnsRestrictions() && !hasRegularColumnsRestrictions();
    }

    /**
     * Determines if the query should return the static content when a partition without rows is returned (as a
     * result set row with null for all other regular columns.)
     *
     * @return {@code true} if the query should return the static content when a partition without rows is returned,
     * {@code false} otherwise.
     */
    public boolean returnStaticContentOnPartitionWithNoRows()
    {
        if (table.isStaticCompactTable())
            return true;

        // The general rationale is that if some rows are specifically selected by the query (have clustering or
        // regular columns restrictions), we ignore partitions that are empty outside of static content, but if it's
        // a full partition query, then we include that content.
        return queriesFullPartitions();
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    private static String allowFilteringMessage(ClientState state)
    {
        return Guardrails.allowFilteringEnabled.isEnabled(state)
               ? REQUIRES_ALLOW_FILTERING_MESSAGE
               : CANNOT_USE_ALLOW_FILTERING_MESSAGE;
    }

    /**
     * One OR subtree of the WHERE clause, with the restriction prepared for each of its relation
     * leaves. Leaves are identified by element identity because two leaves may be structurally
     * equal, like {@code a = 1 OR a = 1}.
     */
    private static final class DisjunctionHolder
    {
        private final WhereClause.OrElement element;
        private final Map<WhereClause.RelationElement, SingleRestriction> leafRestrictions;
        // The first relation on a column of an AND group, to the restriction merged from all of them
        private final Map<WhereClause.RelationElement, SingleRestriction> mergedRestrictions = new IdentityHashMap<>();
        // The later relations on such a column, emitted through the merged restriction
        private final Set<WhereClause.RelationElement> absorbedLeaves = Collections.newSetFromMap(new IdentityHashMap<>());

        private DisjunctionHolder(WhereClause.OrElement element,
                                  Map<WhereClause.RelationElement, SingleRestriction> leafRestrictions)
        {
            this.element = element;
            this.leafRestrictions = leafRestrictions;
        }
    }
}
