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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.postings.PostingListWithPositions;
import org.apache.cassandra.index.sai.utils.IndexEntry;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SegmentRamBufferTest extends SAIRandomizedTester
{
    @Test
    public void shouldReturnValuesInSortedValue()
    {
        int numRows = nextInt(100, 1000);

        // Generate a random unsorted list of integers
        List<Integer> values = IntStream.generate(() -> nextInt(0, 1000))
                                        .distinct()
                                        .limit(numRows)
                                        .boxed()
                                        .collect(Collectors.toList());

        SegmentTrieBuffer buffer = new SegmentTrieBuffer();

        IndexTermType indexTermType = createIndexTermType(Int32Type.instance);

        values.forEach(value -> buffer.add(v -> indexTermType.asComparableBytes(Int32Type.instance.decompose(value), v), Integer.BYTES, 0));

        Iterable<IndexEntry> iterable = buffer::iterator;

        List<Integer> result = StreamSupport.stream(iterable.spliterator(), false).mapToInt(pair -> unpackValue(pair.term)).boxed().collect(Collectors.toList());

        Collections.sort(values);

        assertEquals(values, result);
    }

    @Test
    public void analyzedBufferAccountsPositionsAndReplaysThem() throws Exception
    {
        SegmentTrieBuffer buffer = new SegmentTrieBuffer(true);

        IndexTermType indexTermType = createIndexTermType(UTF8Type.instance);
        ByteComparable term = v -> indexTermType.asComparableBytes(UTF8Type.instance.decompose("term"), v);

        long memoryBefore = buffer.memoryUsed();
        assertTrue(buffer.add(term, 4, 0, new int[]{ 0, 2 }) > 0);
        buffer.add(term, 4, 1, new int[]{ 5 });
        assertTrue(buffer.memoryUsed() > memoryBefore);

        memoryBefore = buffer.memoryUsed();
        assertTrue(buffer.recordDocLength(0, 3) > 0);
        buffer.recordDocLength(1, 1);
        assertTrue(buffer.memoryUsed() > memoryBefore);
        assertArrayEquals(new int[]{ 3, 1, 0 }, buffer.docLengths(3));

        Iterator<IndexEntry> iterator = buffer.iterator();
        assertTrue(iterator.hasNext());
        IndexEntry entry = iterator.next();
        PostingListWithPositions postings = (PostingListWithPositions) entry.postingList;
        assertEquals(0, postings.nextPosting());
        assertArrayEquals(new int[]{ 0, 2 }, postings.positionsForCurrentPosting());
        assertEquals(1, postings.nextPosting());
        assertArrayEquals(new int[]{ 5 }, postings.positionsForCurrentPosting());
        assertEquals(PostingList.END_OF_STREAM, postings.nextPosting());
        assertFalse(iterator.hasNext());

        // The per-posting position counts must be accounted too. The packed builders preallocate
        // whole pages charged with the first add, so after it the only per-posting growth below a
        // page boundary is the position counts backing array.
        SegmentTrieBuffer accountingBuffer = new SegmentTrieBuffer(true);
        accountingBuffer.add(term, 4, 0, new int[]{ 0 });
        int postingCount = 512;
        long accounted = 0;
        for (int i = 1; i <= postingCount; i++)
            accounted += accountingBuffer.add(term, 4, i, new int[]{ i });
        assertTrue(accounted >= 4L * (postingCount - 128));
    }

    private static int unpackValue(ByteComparable value)
    {
        return Int32Type.instance.compose(Int32Type.instance.fromComparableBytes(ByteSource.peekable(value.asComparableBytes(ByteComparable.Version.OSS50)),
                                                                                 ByteComparable.Version.OSS50));
    }
}
