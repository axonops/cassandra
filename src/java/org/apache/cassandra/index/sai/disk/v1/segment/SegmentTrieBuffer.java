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
package org.apache.cassandra.index.sai.disk.v1.segment;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.postings.PostingListWithPositions;
import org.apache.cassandra.index.sai.utils.IndexEntry;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/**
 * On-heap buffer for values that provides a sorted view of itself as an {@link Iterator}.
 * <p>
 * A buffer built for an analyzed index additionally accumulates each posting's token positions,
 * packed like the postings themselves, and the per-row document lengths, and replays the positions
 * through {@link PostingListWithPositions} when iterated.
 */
@NotThreadSafe
public class SegmentTrieBuffer
{
    private static final int MAX_RECURSIVE_TERM_LENGTH = 128;

    private final InMemoryTrie<PostingsBuffer> trie;
    private final PostingsAccumulator postingsAccumulator;
    private final boolean analyzed;
    private int numRows;
    private int[] docLengths;
    private long docLengthsHeapUsed;

    public SegmentTrieBuffer()
    {
        this(false);
    }

    public SegmentTrieBuffer(boolean analyzed)
    {
        trie = new InMemoryTrie<>(DatabaseDescriptor.getMemtableAllocationType().toBufferType());
        postingsAccumulator = new PostingsAccumulator(analyzed);
        this.analyzed = analyzed;
    }

    public int numRows()
    {
        return numRows;
    }

    public long memoryUsed()
    {
        return trie.sizeOnHeap() + postingsAccumulator.heapAllocations() + docLengthsHeapUsed;
    }

    public long add(ByteComparable term, int termLength, int segmentRowId)
    {
        return add(term, termLength, segmentRowId, null);
    }

    public long add(ByteComparable term, int termLength, int segmentRowId, @Nullable int[] positions)
    {
        final long initialSizeOnHeap = trie.sizeOnHeap();
        final long reducerHeapSize = postingsAccumulator.heapAllocations();

        postingsAccumulator.prepare(positions);
        try
        {
            trie.putSingleton(term, segmentRowId, postingsAccumulator, termLength <= MAX_RECURSIVE_TERM_LENGTH);
        }
        catch (InMemoryTrie.SpaceExhaustedException e)
        {
            throw Throwables.unchecked(e);
        }

        numRows++;
        return (trie.sizeOnHeap() - initialSizeOnHeap) + (postingsAccumulator.heapAllocations() - reducerHeapSize);
    }

    /**
     * Records one row's total token count, indexed by segment row id, and returns the heap the
     * call allocated
     */
    public long recordDocLength(int segmentRowId, int tokenCount)
    {
        long allocated = 0;
        if (docLengths == null)
        {
            docLengths = new int[Math.max(64, segmentRowId + 1)];
            allocated = 4L * docLengths.length;
        }
        else if (segmentRowId >= docLengths.length)
        {
            int previousLength = docLengths.length;
            docLengths = Arrays.copyOf(docLengths, Math.max(segmentRowId + 1, previousLength * 2));
            allocated = 4L * (docLengths.length - previousLength);
        }
        docLengths[segmentRowId] = tokenCount;
        docLengthsHeapUsed += allocated;
        return allocated;
    }

    /**
     * @return the recorded document lengths as an array of the given size, zero meaning no indexed
     * value in that row
     */
    public int[] docLengths(int size)
    {
        return docLengths == null ? new int[size] : Arrays.copyOf(docLengths, size);
    }

    public Iterator<IndexEntry> iterator()
    {
        Iterator<Map.Entry<ByteComparable, PostingsBuffer>> iterator = trie.entrySet().iterator();

        return new Iterator<>()
        {
            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public IndexEntry next()
            {
                Map.Entry<ByteComparable, PostingsBuffer> entry = iterator.next();
                return IndexEntry.create(entry.getKey(), analyzed ? analyzedPostings(entry.getValue())
                                                                  : postings(entry.getValue()));
            }
        };
    }

    private static PostingList postings(PostingsBuffer buffer)
    {
        PackedLongValues postings = buffer.rowIds.build();
        PackedLongValues.Iterator postingsIterator = postings.iterator();
        return new PostingList()
        {
            @Override
            public long nextPosting()
            {
                if (postingsIterator.hasNext())
                    return postingsIterator.next();
                return END_OF_STREAM;
            }

            @Override
            public long size()
            {
                return postings.size();
            }

            @Override
            public long advance(long targetRowID)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static PostingListWithPositions analyzedPostings(PostingsBuffer buffer)
    {
        PackedLongValues postings = buffer.rowIds.build();
        PackedLongValues.Iterator postingsIterator = postings.iterator();
        PackedLongValues positions = buffer.positions.build();
        PackedLongValues.Iterator positionsIterator = positions.iterator();
        IntArrayList positionCounts = buffer.positionCounts;
        return new PostingListWithPositions()
        {
            private int index = -1;
            private int[] currentPositions;

            @Override
            public long nextPosting()
            {
                if (!postingsIterator.hasNext())
                    return END_OF_STREAM;

                index++;
                currentPositions = new int[positionCounts.get(index)];
                for (int i = 0; i < currentPositions.length; i++)
                    currentPositions[i] = Math.toIntExact(positionsIterator.next());
                return postingsIterator.next();
            }

            @Override
            public int[] positionsForCurrentPosting()
            {
                return currentPositions;
            }

            @Override
            public long size()
            {
                return postings.size();
            }

            @Override
            public long advance(long targetRowID)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * One term's buffered postings: the delta-packed row ids and, for analyzed indexes, all the
     * postings' positions concatenated in the same packed shape with one count per posting
     */
    private static class PostingsBuffer
    {
        // object header plus the backing array header of the position counts list
        private static final long POSITION_COUNTS_OVERHEAD = 32;

        final PackedLongValues.Builder rowIds = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
        @Nullable
        PackedLongValues.Builder positions;
        @Nullable
        IntArrayList positionCounts;

        long ramBytesUsed()
        {
            return rowIds.ramBytesUsed()
                   + (positions == null ? 0 : positions.ramBytesUsed())
                   + (positionCounts == null ? 0 : POSITION_COUNTS_OVERHEAD + 4L * positionCounts.buffer.length);
        }
    }

    private static class PostingsAccumulator implements InMemoryTrie.UpsertTransformer<PostingsBuffer, Integer>
    {
        private final LongAdder heapAllocations = new LongAdder();
        private final boolean analyzed;

        // Set immediately before each trie upsert. Safe because the buffer is single-writer.
        private int[] positions;

        PostingsAccumulator(boolean analyzed)
        {
            this.analyzed = analyzed;
        }

        void prepare(int[] positions)
        {
            this.positions = positions;
        }

        @Override
        public PostingsBuffer apply(PostingsBuffer existing, Integer rowID)
        {
            if (existing == null)
            {
                existing = new PostingsBuffer();
                if (analyzed)
                {
                    existing.positions = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
                    existing.positionCounts = new IntArrayList();
                }
                heapAllocations.add(existing.ramBytesUsed());
            }
            long ramBefore = existing.ramBytesUsed();
            existing.rowIds.add(rowID);
            if (analyzed)
            {
                for (int position : positions)
                    existing.positions.add(position);
                existing.positionCounts.add(positions.length);
            }
            heapAllocations.add(existing.ramBytesUsed() - ramBefore);
            return existing;
        }

        long heapAllocations()
        {
            return heapAllocations.longValue();
        }
    }
}
