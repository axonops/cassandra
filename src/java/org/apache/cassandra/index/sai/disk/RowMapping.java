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
package org.apache.cassandra.index.sai.disk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import javax.annotation.concurrent.NotThreadSafe;

import com.carrotsearch.hppc.LongArrayList;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.memory.TrieMemoryIndex;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.index.sai.utils.PrimaryKeysWithPositions;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.AbstractGuavaIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * In memory representation of {@link PrimaryKey} to row ID mappings which only contains
 * {@link Row} regardless of whether it's live or deleted. ({@link RangeTombstoneMarker} is not included.)
 * <p>
 * While this inherits the threading behaviour of {@link InMemoryTrie} of single-writer / multiple-reader,
 * since it is only used by {@link StorageAttachedIndexWriter}, which is not threadsafe, we can consider
 * this class not threadsafe as well.
 */
@NotThreadSafe
public class RowMapping
{
    private static final InMemoryTrie.UpsertTransformer<Long, Long> OVERWRITE_TRANSFORMER = (existing, update) -> update;

    public static final RowMapping DUMMY = new RowMapping()
    {
        @Override
        public Iterator<Pair<ByteComparable, LongArrayList>> merge(MemtableIndex index) { return Collections.emptyIterator(); }

        @Override
        public Iterator<Pair<ByteComparable, TermPostings>> mergeAnalyzed(MemtableIndex index, boolean isCollection) { return Collections.emptyIterator(); }

        @Override
        public void complete() {}

        @Override
        public boolean isComplete()
        {
            return true;
        }

        @Override
        public void add(PrimaryKey key, long sstableRowId) {}

        @Override
        public int get(PrimaryKey key)
        {
            return -1;
        }
    };

    private final InMemoryTrie<Long> rowMapping = new InMemoryTrie<>(BufferType.OFF_HEAP);

    private boolean complete = false;

    private RowMapping()
    {}

    /**
     * Create row mapping for FLUSH operation only.
     */
    public static RowMapping create(OperationType opType)
    {
        if (opType == OperationType.FLUSH)
            return new RowMapping();
        return DUMMY;
    }

    /**
     * Link the term -> {@link PrimaryKeys} mappings from a provided {@link MemtableIndex} to
     * the {@link PrimaryKey} -> row ID mappings maintained here in {@link #rowMapping} to produce
     * mappings of terms to their postings lists.
     *
     * @param index a Memtable-attached column index
     *
     * @return an iterator of term -> postings list {@link Pair}s
     */
    public Iterator<Pair<ByteComparable, LongArrayList>> merge(MemtableIndex index)
    {
        assert complete : "RowMapping is not built.";

        Iterator<Pair<ByteComparable, PrimaryKeys>> iterator = index.iterator();
        return new AbstractGuavaIterator<>()
        {
            @Override
            protected Pair<ByteComparable, LongArrayList> computeNext()
            {
                while (iterator.hasNext())
                {
                    Pair<ByteComparable, PrimaryKeys> pair = iterator.next();

                    LongArrayList postings = null;
                    Iterator<PrimaryKey> primaryKeys = pair.right.iterator();

                    while (primaryKeys.hasNext())
                    {
                        Long sstableRowId = rowMapping.get(primaryKeys.next());

                        // The in-memory index does not handle deletions, so it is possible to
                        // have a primary key in the index that doesn't exist in the row mapping
                        if (sstableRowId != null)
                        {
                            postings = postings == null ? new LongArrayList() : postings;
                            postings.add(sstableRowId);
                        }
                    }
                    if (postings != null)
                        return Pair.create(pair.left, postings);
                }
                return endOfData();
            }
        };
    }

    /**
     * The analyzed variant of {@link #merge(MemtableIndex)}: the term to primary key mappings carry
     * positions, so each term produces aligned row ids and per-posting position arrays.
     * <p>
     * For scalar columns, entries whose epoch is older than the primary key's newest epoch in the
     * memtable's doc-length map are stale remnants of an overwritten value and are dropped. A
     * collection column's older epochs are still live data from earlier partial updates, so
     * collections keep add-only semantics.
     *
     * @param index a Memtable-attached column index with positions
     * @param isCollection whether the index is over a non-frozen collection
     *
     * @return an iterator of term to {@link TermPostings} {@link Pair}s
     */
    public Iterator<Pair<ByteComparable, TermPostings>> mergeAnalyzed(MemtableIndex index, boolean isCollection)
    {
        assert complete : "RowMapping is not built.";

        NavigableMap<PrimaryKey, TrieMemoryIndex.DocLength> docLengths = index.docLengths();
        Iterator<Pair<ByteComparable, PrimaryKeys>> iterator = index.iterator();
        return new AbstractGuavaIterator<>()
        {
            @Override
            protected Pair<ByteComparable, TermPostings> computeNext()
            {
                while (iterator.hasNext())
                {
                    Pair<ByteComparable, PrimaryKeys> pair = iterator.next();

                    TermPostings postings = null;
                    Iterator<Map.Entry<PrimaryKey, PrimaryKeysWithPositions.PositionEntry>> entries =
                        ((PrimaryKeysWithPositions) pair.right).entries();

                    while (entries.hasNext())
                    {
                        Map.Entry<PrimaryKey, PrimaryKeysWithPositions.PositionEntry> entry = entries.next();

                        // The in-memory index does not handle deletions, so it is possible to
                        // have a primary key in the index that doesn't exist in the row mapping
                        Long sstableRowId = rowMapping.get(entry.getKey());
                        if (sstableRowId == null)
                            continue;

                        // A scalar overwrite fully replaces the value, so an older epoch proves the
                        // term is stale for this key
                        if (!isCollection)
                        {
                            TrieMemoryIndex.DocLength docLength = docLengths.get(entry.getKey());
                            if (docLength == null || docLength.epoch != entry.getValue().epoch())
                                continue;
                        }

                        postings = postings == null ? new TermPostings() : postings;
                        postings.add(sstableRowId, entry.getValue().positions());
                    }
                    if (postings != null)
                        return Pair.create(pair.left, postings);
                }
                return endOfData();
            }
        };
    }

    /**
     * Complete building in memory RowMapping, mark it as immutable.
     */
    public void complete()
    {
        assert !complete : "RowMapping can only be built once.";
        this.complete = true;
    }

    public boolean isComplete()
    {
        return complete;
    }

    /**
     * Include PrimaryKey to RowId mapping
     */
    public void add(PrimaryKey key, long sstableRowId) throws InMemoryTrie.SpaceExhaustedException
    {
        assert !complete : "Cannot modify and already built RowMapping.";
        rowMapping.putSingleton(key, sstableRowId, OVERWRITE_TRANSFORMER);
    }

    /**
     * Returns the SSTable row ID for a {@link PrimaryKey}
     *
     * @param key the {@link PrimaryKey}
     * @return a valid SSTable row ID for the {@link PrimaryKey} or -1 if the {@link PrimaryKey} doesn't exist
     * in the {@link RowMapping}
     */
    public int get(PrimaryKey key)
    {
        Long sstableRowId = rowMapping.get(key);
        return sstableRowId == null ? -1 : Math.toIntExact(sstableRowId);
    }

    /**
     * One term's postings from an analyzed memtable index: sstable row ids in ascending order with
     * the aligned per-posting position arrays
     */
    public static class TermPostings
    {
        public final LongArrayList rowIds = new LongArrayList();
        public final List<int[]> positions = new ArrayList<>();

        void add(long rowId, int[] postingPositions)
        {
            rowIds.add(rowId);
            positions.add(postingPositions);
        }

        public int size()
        {
            return rowIds.size();
        }
    }
}
