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
package org.apache.cassandra.index.sai.disk.v1.postings;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.postings.PostingListWithPositions;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.lucene.store.IndexInput;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Round-trips positions through the real writers and reads them back by posting ordinal. The
 * randomized posting counts cross the 128 posting block boundary and the randomized row id spans
 * vary the skip table's packed widths, pinning the DirectWriter byte arithmetic
 * {@link PositionsReader#readPositionsSummaryOffset} skips with.
 */
public class PositionsReaderTest extends SAIRandomizedTester
{
    private IndexDescriptor indexDescriptor;
    private IndexIdentifier indexIdentifier;

    @Before
    public void setup() throws Throwable
    {
        indexDescriptor = newIndexDescriptor();
        String index = newIndex();
        indexIdentifier = SAITester.createIndexIdentifier(indexDescriptor.sstableDescriptor.ksname,
                                                          indexDescriptor.sstableDescriptor.cfname,
                                                          index);
    }

    @Test
    public void randomizedPositionsByOrdinalRoundtrip() throws Exception
    {
        int numTerms = nextInt(1, 10);
        long[][] termPostings = new long[numTerms][];
        int[][][] termPositions = new int[numTerms][][];
        long[] postingsSummaryOffsets = new long[numTerms];

        try (PositionsWriter positionsWriter = new PositionsWriter(indexDescriptor, indexIdentifier);
             PostingsWriter postingsWriter = new PostingsWriter(indexDescriptor, indexIdentifier, positionsWriter))
        {
            for (int term = 0; term < numTerms; term++)
            {
                termPostings[term] = randomPostings();
                termPositions[term] = randomPositions(termPostings[term].length);
                postingsSummaryOffsets[term] = postingsWriter.write(postingList(termPostings[term], termPositions[term]));
            }
            positionsWriter.writeDocLengths(new int[]{ 1 });
            positionsWriter.complete();
            postingsWriter.complete();
        }

        try (IndexInput postingsInput = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier))
        {
            for (int term = 0; term < numTerms; term++)
            {
                long positionsSummaryOffset = PositionsReader.readPositionsSummaryOffset(postingsInput, postingsSummaryOffsets[term]);
                // the reader owns and closes its input, so every term opens its own
                try (PositionsReader reader = new PositionsReader(indexDescriptor.openPerIndexInput(IndexComponent.POSITIONS, indexIdentifier),
                                                                  positionsSummaryOffset))
                {
                    assertEquals(termPostings[term].length, reader.size());

                    // read ordinals in shuffled order to exercise random access
                    List<Integer> ordinals = shuffledOrdinals(termPostings[term].length);
                    for (int ordinal : ordinals)
                    {
                        assertEquals(termPositions[term][ordinal].length, reader.frequency(ordinal));
                        assertArrayEquals(termPositions[term][ordinal], reader.positions(ordinal));
                    }
                }
            }
        }
    }

    @Test
    public void phrasePostingListIntersectsOnPositions() throws Exception
    {
        // Rows: 10 'quick brown fox', 20 'brown quick fox', 30 'quick fox', 40 'quick brown fox'
        long[] quickPostings = { 10, 20, 30, 40 };
        int[][] quickPositions = { { 0 }, { 1 }, { 0 }, { 0 } };
        long[] brownPostings = { 10, 20, 40 };
        int[][] brownPositions = { { 1 }, { 0 }, { 1 } };

        long quickOffset;
        long brownOffset;
        try (PositionsWriter positionsWriter = new PositionsWriter(indexDescriptor, indexIdentifier);
             PostingsWriter postingsWriter = new PostingsWriter(indexDescriptor, indexIdentifier, positionsWriter))
        {
            quickOffset = postingsWriter.write(postingList(quickPostings, quickPositions));
            brownOffset = postingsWriter.write(postingList(brownPostings, brownPositions));
            positionsWriter.writeDocLengths(new int[]{ 3 });
            positionsWriter.complete();
            postingsWriter.complete();
        }

        // The phrase 'quick brown' must keep rows 10 and 40 only
        PostingsReader[] postings = new PostingsReader[2];
        PositionsReader[] positions = new PositionsReader[2];
        IndexInput quickSummary = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier);
        IndexInput brownSummary = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier);
        postings[0] = new PostingsReader(indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier),
                                         new PostingsReader.BlocksSummary(quickSummary, quickOffset),
                                         QueryEventListener.PostingListEventListener.NO_OP);
        postings[1] = new PostingsReader(indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier),
                                         new PostingsReader.BlocksSummary(brownSummary, brownOffset),
                                         QueryEventListener.PostingListEventListener.NO_OP);
        try (IndexInput postingsInput = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier))
        {
            positions[0] = new PositionsReader(indexDescriptor.openPerIndexInput(IndexComponent.POSITIONS, indexIdentifier),
                                               PositionsReader.readPositionsSummaryOffset(postingsInput, quickOffset));
            positions[1] = new PositionsReader(indexDescriptor.openPerIndexInput(IndexComponent.POSITIONS, indexIdentifier),
                                               PositionsReader.readPositionsSummaryOffset(postingsInput, brownOffset));
        }

        try (PostingList phrase = new PhrasePostingList(indexIdentifier, postings, positions, new int[]{ 0, 1 }, new int[]{ 0, 1 }))
        {
            assertEquals(10, phrase.nextPosting());
            assertEquals(40, phrase.nextPosting());
            assertEquals(PostingList.END_OF_STREAM, phrase.nextPosting());
        }
    }

    private List<Integer> shuffledOrdinals(int count)
    {
        List<Integer> ordinals = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
            ordinals.add(i);
        for (int i = count - 1; i > 0; i--)
        {
            int swap = nextInt(0, i + 1);
            Integer tmp = ordinals.get(i);
            ordinals.set(i, ordinals.get(swap));
            ordinals.set(swap, tmp);
        }
        return ordinals;
    }

    private long[] randomPostings()
    {
        // Crossing the 128 posting block size matters for the summary's skip table shape
        int count = nextInt(1, 400);
        long[] postings = new long[count];
        long rowId = 0;
        for (int i = 0; i < count; i++)
        {
            rowId += nextInt(1, 100000);
            postings[i] = rowId;
        }
        return postings;
    }

    private int[][] randomPositions(int postings)
    {
        int[][] positions = new int[postings][];
        for (int posting = 0; posting < postings; posting++)
        {
            int frequency = nextInt(1, 6);
            positions[posting] = new int[frequency];
            int position = nextInt(0, 3);
            for (int i = 0; i < frequency; i++)
            {
                positions[posting][i] = position;
                position += nextInt(1, 70000);
            }
        }
        return positions;
    }

    private static PostingListWithPositions postingList(long[] postings, int[][] positions)
    {
        return new PostingListWithPositions()
        {
            private int index = -1;

            @Override
            public long nextPosting()
            {
                return ++index < postings.length ? postings[index] : PostingList.END_OF_STREAM;
            }

            @Override
            public int[] positionsForCurrentPosting()
            {
                return positions[index];
            }

            @Override
            public long size()
            {
                return postings.length;
            }

            @Override
            public long advance(long targetRowID)
            {
                long posting;
                while ((posting = nextPosting()) != PostingList.END_OF_STREAM)
                {
                    if (posting >= targetRowID)
                        return posting;
                }
                return PostingList.END_OF_STREAM;
            }
        };
    }
}
