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
package org.apache.cassandra.index.sai.memory;

import java.nio.ByteBuffer;
import java.util.Iterator;

import com.google.common.base.Preconditions;

import org.apache.cassandra.index.sai.disk.RowMapping;
import org.apache.cassandra.index.sai.postings.PostingListWithPositions;
import org.apache.cassandra.index.sai.utils.IndexEntry;
import org.apache.cassandra.index.sai.utils.TermsIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * The analyzed twin of {@link MemtableTermsIterator}: adapts the positional merge output of
 * {@link RowMapping#mergeAnalyzed} for flushing, exposing each term's postings as a
 * {@link PostingListWithPositions} so the segment writer can stream positions next to postings.
 */
public class AnalyzedMemtableTermsIterator implements TermsIterator
{
    private final ByteBuffer minTerm;
    private final ByteBuffer maxTerm;
    private final Iterator<Pair<ByteComparable, RowMapping.TermPostings>> iterator;

    private Pair<ByteComparable, RowMapping.TermPostings> current;

    private long maxSSTableRowId = -1;
    private long minSSTableRowId = Long.MAX_VALUE;

    public AnalyzedMemtableTermsIterator(ByteBuffer minTerm,
                                         ByteBuffer maxTerm,
                                         Iterator<Pair<ByteComparable, RowMapping.TermPostings>> iterator)
    {
        Preconditions.checkArgument(iterator != null);
        this.minTerm = minTerm;
        this.maxTerm = maxTerm;
        this.iterator = iterator;
    }

    @Override
    public ByteBuffer getMinTerm()
    {
        return minTerm;
    }

    @Override
    public ByteBuffer getMaxTerm()
    {
        return maxTerm;
    }

    @Override
    public void close() {}

    @Override
    public boolean hasNext()
    {
        return iterator.hasNext();
    }

    @Override
    public IndexEntry next()
    {
        current = iterator.next();
        return IndexEntry.create(current.left, postings());
    }

    public long getMaxSSTableRowId()
    {
        return maxSSTableRowId;
    }

    public long getMinSSTableRowId()
    {
        return minSSTableRowId;
    }

    private PostingListWithPositions postings()
    {
        final RowMapping.TermPostings termPostings = current.right;

        assert termPostings.size() > 0;

        minSSTableRowId = Math.min(minSSTableRowId, termPostings.rowIds.get(0));
        maxSSTableRowId = Math.max(maxSSTableRowId, termPostings.rowIds.get(termPostings.size() - 1));

        return new PostingListWithPositions()
        {
            private int index = -1;

            @Override
            public long nextPosting()
            {
                if (index + 1 == termPostings.size())
                {
                    return END_OF_STREAM;
                }

                return termPostings.rowIds.get(++index);
            }

            @Override
            public int[] positionsForCurrentPosting()
            {
                return termPostings.positions.get(index);
            }

            @Override
            public long size()
            {
                return termPostings.size();
            }

            @Override
            public long advance(long targetRowID)
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
