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

import org.agrona.collections.LongArrayList;
import org.apache.cassandra.index.sai.disk.ResettableByteBuffersIndexOutput;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.DirectWriter;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Writes the {@link IndexComponent#POSITIONS} segment frame for an analyzed index, in the same term
 * loop that writes postings.
 * <p>
 * Per term the layout is a positions payload followed by a term summary. The payload holds each
 * posting's positions delta-encoded as VInts (first value absolute, then gaps), concatenated in
 * posting order. The summary holds the absolute file offset of the payload, the posting count, and
 * two DirectWriter-packed prefix-sum arrays: cumulative position counts, so {@code freq(i)} reads
 * without touching payload bytes, and cumulative payload byte lengths, so any posting's positions
 * seek in constant time. The summary's file offset travels as one extra VLong at the end of the
 * postings summary written by {@link PostingsWriter}.
 * <p>
 * After the last term the segment's document lengths are written at the frame tail as one
 * DirectWriter-packed array indexed by segment row id, zero meaning no indexed value in that row.
 *
 * Visual representation of the disk format:
 * <pre>
 *
 * +========+==========================+=====+==========================+=============+========+
 * | HEADER | POSITIONS LIST (TERM 1)  | ... | POSITIONS LIST (TERM N)  | DOC LENGTHS | FOOTER |
 * +========+==========================+=====+==========================+=============+========+
 *          | POSITION DELTAS | TERM SUMMARY               |
 *          +-----------------+----------------------------+
 *                            | PAYLOAD OFFSET             |
 *                            | POSTING COUNT              |
 *                            | FREQUENCY BLOCK LENGTH     |
 *                            | FREQUENCY PREFIX SUMS      |
 *                            | BYTE OFFSET PREFIX SUMS    |
 *                            +----------------------------+
 *
 * </pre>
 */
@NotThreadSafe
public class PositionsWriter implements Closeable
{
    private final IndexOutputWriter dataOutput;
    private final long startOffset;
    private final LongArrayList cumulativeFrequencies = new LongArrayList();
    private final LongArrayList cumulativeBytes = new LongArrayList();
    private final ResettableByteBuffersIndexOutput inMemoryOutput = new ResettableByteBuffersIndexOutput("frequencyPrefixSums");

    private long payloadStart = -1;
    private long totalFrequency;
    private long totalBytes;

    private long docLengthsOffset = -1;
    private long docCount;
    private long sumDocLengths;

    public PositionsWriter(IndexDescriptor indexDescriptor, IndexIdentifier indexIdentifier) throws IOException
    {
        this.dataOutput = indexDescriptor.openPerIndexOutput(IndexComponent.POSITIONS, indexIdentifier, true);
        startOffset = dataOutput.getFilePointer();
        SAICodecUtils.writeHeader(dataOutput, Version.AB);
    }

    /**
     * @return current file pointer
     */
    public long getFilePointer()
    {
        return dataOutput.getFilePointer();
    }

    /**
     * @return file pointer where index structure begins (before header)
     */
    public long getStartOffset()
    {
        return startOffset;
    }

    /**
     * Appends one posting's positions to the current term's payload. Called once per posting, in
     * posting order.
     */
    public void addPositions(int[] positions) throws IOException
    {
        checkArgument(positions != null && positions.length > 0, "Expected a non-empty positions array.");

        if (payloadStart == -1)
            payloadStart = dataOutput.getFilePointer();

        long bytesBefore = dataOutput.getFilePointer();
        int previous = 0;
        for (int index = 0; index < positions.length; index++)
        {
            dataOutput.writeVInt(index == 0 ? positions[0] : positions[index] - previous);
            previous = positions[index];
        }

        totalFrequency += positions.length;
        totalBytes += dataOutput.getFilePointer() - bytesBefore;
        cumulativeFrequencies.add(totalFrequency);
        cumulativeBytes.add(totalBytes);
    }

    /**
     * Writes the current term's summary after its last posting.
     *
     * @return file offset to the summary of this term's positions
     */
    public long completeTerm() throws IOException
    {
        assert !cumulativeFrequencies.isEmpty() : "No positions were written for the term";

        final long summaryOffset = dataOutput.getFilePointer();
        dataOutput.writeVLong(payloadStart);
        dataOutput.writeVInt(cumulativeFrequencies.size());

        // compressing the frequency prefix sums in memory first, to know the exact length (with padding)
        inMemoryOutput.reset();
        writeSortedFoRBlock(cumulativeFrequencies, inMemoryOutput);
        dataOutput.writeVLong(inMemoryOutput.getFilePointer());
        inMemoryOutput.copyTo(dataOutput);
        writeSortedFoRBlock(cumulativeBytes, dataOutput);

        payloadStart = -1;
        totalFrequency = 0;
        totalBytes = 0;
        cumulativeFrequencies.clear();
        cumulativeBytes.clear();
        return summaryOffset;
    }

    /**
     * Writes the segment's document lengths at the frame tail, indexed by segment row id, and
     * computes the aggregates persisted in the component metadata attributes.
     */
    public void writeDocLengths(int[] docLengths) throws IOException
    {
        docLengthsOffset = dataOutput.getFilePointer();

        long maxLength = 0;
        for (int docLength : docLengths)
        {
            if (docLength > 0)
            {
                docCount++;
                sumDocLengths += docLength;
                maxLength = Math.max(maxLength, docLength);
            }
        }

        dataOutput.writeVInt(docLengths.length);
        final int bitsPerValue = maxLength == 0 ? 0 : DirectWriter.unsignedBitsRequired(maxLength);
        dataOutput.writeByte((byte) bitsPerValue);
        if (bitsPerValue > 0)
        {
            final DirectWriter writer = DirectWriter.getInstance(dataOutput, docLengths.length, bitsPerValue);
            for (int docLength : docLengths)
            {
                writer.add(docLength);
            }
            writer.finish();
        }
    }

    /**
     * write footer to the positions
     */
    public void complete() throws IOException
    {
        SAICodecUtils.writeFooter(dataOutput);
    }

    @Override
    public void close() throws IOException
    {
        dataOutput.close();
    }

    /**
     * @return the file offset of the packed doc-length array, only valid after {@link #writeDocLengths}
     */
    public long getDocLengthsOffset()
    {
        return docLengthsOffset;
    }

    /**
     * @return the number of rows with an indexed value, only valid after {@link #writeDocLengths}
     */
    public long getDocCount()
    {
        return docCount;
    }

    /**
     * @return the sum of all document lengths, only valid after {@link #writeDocLengths}
     */
    public long getSumDocLengths()
    {
        return sumDocLengths;
    }

    private void writeSortedFoRBlock(LongArrayList values, IndexOutput output) throws IOException
    {
        assert values.size() > 0;
        final long maxValue = values.getLong(values.size() - 1);

        final int bitsPerValue = maxValue == 0 ? 0 : DirectWriter.unsignedBitsRequired(maxValue);
        output.writeByte((byte) bitsPerValue);
        if (bitsPerValue > 0)
        {
            final DirectWriter writer = DirectWriter.getInstance(output, values.size(), bitsPerValue);
            for (int i = 0; i < values.size(); ++i)
            {
                writer.add(values.getLong(i));
            }
            writer.finish();
        }
    }
}
