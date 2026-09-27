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

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.SeekingRandomAccessInput;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectReader;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class PositionsWriterTest extends SAIRandomizedTester
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
    public void testRandomizedPositionsRoundtrip() throws Exception
    {
        int numTerms = nextInt(1, 20);
        int[][][] termPositions = new int[numTerms][][];
        long[] summaryOffsets = new long[numTerms];
        int[] docLengths = randomDocLengths();
        long docLengthsOffset;
        long docCount;
        long sumDocLengths;

        try (PositionsWriter writer = new PositionsWriter(indexDescriptor, indexIdentifier))
        {
            for (int term = 0; term < numTerms; term++)
            {
                termPositions[term] = randomTermPositions();
                for (int[] positions : termPositions[term])
                    writer.addPositions(positions);
                summaryOffsets[term] = writer.completeTerm();
            }
            writer.writeDocLengths(docLengths);
            writer.complete();
            docLengthsOffset = writer.getDocLengthsOffset();
            docCount = writer.getDocCount();
            sumDocLengths = writer.getSumDocLengths();
        }

        long expectedDocCount = 0;
        long expectedSumDocLengths = 0;
        for (int docLength : docLengths)
        {
            if (docLength > 0)
            {
                expectedDocCount++;
                expectedSumDocLengths += docLength;
            }
        }
        assertEquals(expectedDocCount, docCount);
        assertEquals(expectedSumDocLengths, sumDocLengths);

        try (IndexInput input = indexDescriptor.openPerIndexInput(IndexComponent.POSITIONS, indexIdentifier))
        {
            SAICodecUtils.validate(input);
            SAICodecUtils.validateChecksum(input);

            for (int term = 0; term < numTerms; term++)
                verifyTerm(input, summaryOffsets[term], termPositions[term]);

            verifyDocLengths(input, docLengthsOffset, docLengths);
        }
    }

    @Test
    public void testSingleOccurrenceTerm() throws Exception
    {
        long summaryOffset;
        try (PositionsWriter writer = new PositionsWriter(indexDescriptor, indexIdentifier))
        {
            writer.addPositions(new int[]{ 42 });
            summaryOffset = writer.completeTerm();
            writer.writeDocLengths(new int[]{ 1 });
            writer.complete();
        }

        try (IndexInput input = indexDescriptor.openPerIndexInput(IndexComponent.POSITIONS, indexIdentifier))
        {
            SAICodecUtils.validate(input);
            verifyTerm(input, summaryOffset, new int[][]{ { 42 } });
        }
    }

    private void verifyTerm(IndexInput input, long summaryOffset, int[][] positions) throws IOException
    {
        input.seek(summaryOffset);
        long payloadStart = input.readVLong();
        int numPostings = input.readVInt();
        assertEquals(positions.length, numPostings);

        long frequencyBlockLength = input.readVLong();
        long byteBlockOffset = input.getFilePointer() + frequencyBlockLength;
        long[] cumulativeFrequencies = readSortedFoRBlock(input, numPostings);
        input.seek(byteBlockOffset);
        long[] cumulativeBytes = readSortedFoRBlock(input, numPostings);

        long expectedFrequency = 0;
        for (int posting = 0; posting < numPostings; posting++)
        {
            expectedFrequency += positions[posting].length;
            assertEquals(expectedFrequency, cumulativeFrequencies[posting]);
        }

        // decode each posting's payload through the byte offset prefix sums
        for (int posting = 0; posting < numPostings; posting++)
        {
            long payloadOffset = posting == 0 ? 0 : cumulativeBytes[posting - 1];
            input.seek(payloadStart + payloadOffset);
            int[] decoded = new int[positions[posting].length];
            int previous = 0;
            for (int i = 0; i < decoded.length; i++)
            {
                previous = i == 0 ? input.readVInt() : previous + input.readVInt();
                decoded[i] = previous;
            }
            assertArrayEquals(positions[posting], decoded);
            assertEquals(payloadStart + cumulativeBytes[posting], input.getFilePointer());
        }
    }

    private void verifyDocLengths(IndexInput input, long docLengthsOffset, int[] docLengths) throws IOException
    {
        input.seek(docLengthsOffset);
        assertEquals(docLengths.length, input.readVInt());

        byte bitsPerValue = input.readByte();
        if (bitsPerValue == 0)
        {
            for (int docLength : docLengths)
                assertEquals(0, docLength);
            return;
        }

        SeekingRandomAccessInput randomAccessInput = new SeekingRandomAccessInput(input);
        LongValues values = DirectReader.getInstance(randomAccessInput, bitsPerValue, input.getFilePointer());
        for (int rowId = 0; rowId < docLengths.length; rowId++)
            assertEquals(docLengths[rowId], values.get(rowId));
    }

    private long[] readSortedFoRBlock(IndexInput input, int count) throws IOException
    {
        byte bitsPerValue = input.readByte();
        long[] values = new long[count];
        if (bitsPerValue == 0)
            return values;

        SeekingRandomAccessInput randomAccessInput = new SeekingRandomAccessInput(input);
        LongValues reader = DirectReader.getInstance(randomAccessInput, bitsPerValue, input.getFilePointer());
        for (int i = 0; i < count; i++)
            values[i] = reader.get(i);
        return values;
    }

    private int[][] randomTermPositions()
    {
        int numPostings = nextInt(1, 50);
        int[][] positions = new int[numPostings][];
        for (int posting = 0; posting < numPostings; posting++)
        {
            int frequency = nextInt(1, 8);
            positions[posting] = new int[frequency];
            int position = nextInt(0, 4);
            for (int i = 0; i < frequency; i++)
            {
                positions[posting][i] = position;
                position += nextInt(1, 70000);
            }
        }
        return positions;
    }

    private int[] randomDocLengths()
    {
        int[] docLengths = new int[nextInt(1, 500)];
        for (int i = 0; i < docLengths.length; i++)
            docLengths[i] = getRandom().nextBoolean() ? 0 : nextInt(1, 1000);
        return docLengths;
    }
}
