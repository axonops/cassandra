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

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.disk.io.SeekingRandomAccessInput;
import org.apache.cassandra.index.sai.disk.v1.DirectReaders;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectReader;

/**
 * Reads one term's positions written by {@link PositionsWriter}: parses the term summary, then
 * serves any posting's positions by its ordinal in the term's posting list, the ordinal
 * {@link PostingsReader#getOrdinal()} reports. Frequencies read from the prefix sums without
 * touching payload bytes, and a posting's positions seek in constant time through the byte offset
 * prefix sums.
 */
@NotThreadSafe
public class PositionsReader implements Closeable
{
    private final IndexInput input;
    private final long payloadStart;
    private final int numPostings;
    private final LongValues cumulativeFrequencies;
    private final LongValues cumulativeBytes;

    /**
     * @param input input over the {@link org.apache.cassandra.index.sai.disk.format.IndexComponent#POSITIONS}
     * file, owned and closed by this reader
     * @param summaryOffset absolute file offset of the term's positions summary, from
     * {@link #readPositionsSummaryOffset}
     */
    public PositionsReader(IndexInput input, long summaryOffset) throws IOException
    {
        this.input = input;
        input.seek(summaryOffset);
        payloadStart = input.readVLong();
        numPostings = input.readVInt();

        long frequencyBlockLength = input.readVLong();
        long bytesBlockOffset = input.getFilePointer() + frequencyBlockLength;

        SeekingRandomAccessInput randomAccessInput = new SeekingRandomAccessInput(input);
        byte frequencyBits = input.readByte();
        DirectReaders.checkBitsPerValue(frequencyBits, input, () -> "Positions frequency prefix sums");
        cumulativeFrequencies = frequencyBits == 0 ? LongValues.ZEROES
                                                   : DirectReader.getInstance(randomAccessInput, frequencyBits, input.getFilePointer());

        input.seek(bytesBlockOffset);
        byte bytesBits = input.readByte();
        DirectReaders.checkBitsPerValue(bytesBits, input, () -> "Positions byte offset prefix sums");
        cumulativeBytes = bytesBits == 0 ? LongValues.ZEROES
                                         : DirectReader.getInstance(randomAccessInput, bytesBits, input.getFilePointer());
    }

    /**
     * @return the number of postings the term has, matching the postings summary
     */
    public int size()
    {
        return numPostings;
    }

    /**
     * @param ordinal the posting's zero based position in the term's posting list
     * @return the number of positions the posting stores
     */
    public int frequency(int ordinal)
    {
        long previous = ordinal == 0 ? 0 : cumulativeFrequencies.get(ordinal - 1);
        return Math.toIntExact(cumulativeFrequencies.get(ordinal) - previous);
    }

    /**
     * Reads the ascending token positions of one posting.
     *
     * @param ordinal the posting's zero based position in the term's posting list
     * @return the posting's positions, decoded from the delta encoded payload
     */
    public int[] positions(int ordinal) throws IOException
    {
        int frequency = frequency(ordinal);
        long payloadOffset = ordinal == 0 ? 0 : cumulativeBytes.get(ordinal - 1);
        input.seek(payloadStart + payloadOffset);

        int[] positions = new int[frequency];
        int previous = 0;
        for (int i = 0; i < frequency; i++)
        {
            previous = i == 0 ? input.readVInt() : previous + input.readVInt();
            positions[i] = previous;
        }
        return positions;
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(input);
    }

    /**
     * Reads the trailing VLong the format version ab appends to a postings summary: the absolute
     * offset of the term's positions summary in the positions file. The skip table's packed blocks
     * are skipped with the exact byte counts Lucene's DirectWriter produces.
     *
     * @param input input over the postings file, positioned freely, not closed by this call
     * @param postingsSummaryOffset absolute offset of the term's postings summary
     * @return the absolute offset of the term's positions summary in the positions file
     */
    public static long readPositionsSummaryOffset(IndexInput input, long postingsSummaryOffset) throws IOException
    {
        input.seek(postingsSummaryOffset);
        input.readVInt(); // block size
        input.readVInt(); // number of postings

        int numBlocks = input.readVInt();
        long offsetsBlockLength = input.readVLong();
        input.seek(input.getFilePointer() + offsetsBlockLength);

        byte maxValuesBits = input.readByte();
        DirectReaders.checkBitsPerValue(maxValuesBits, input, () -> "Postings skip table maximum values");
        input.seek(input.getFilePointer() + directWriterBytes(numBlocks, maxValuesBits));
        return input.readVLong();
    }

    /**
     * The exact number of bytes {@code DirectWriter.getInstance(out, count, bits)} writes: the
     * packed values plus the fixed per-width padding DirectWriter appends so DirectReader's wide
     * reads stay in bounds.
     */
    private static long directWriterBytes(long count, byte bits)
    {
        if (bits == 0)
            return 0;
        return (count * bits + 7) / 8 + paddingBytes(bits);
    }

    private static int paddingBytes(byte bits)
    {
        if (bits <= Byte.SIZE)
            return 0;
        if (bits <= Short.SIZE)
            return (Short.SIZE - bits + 7) / 8;
        if (bits <= Integer.SIZE)
            return (Integer.SIZE - bits + 7) / 8;
        return (Long.SIZE - bits + 7) / 8;
    }
}
