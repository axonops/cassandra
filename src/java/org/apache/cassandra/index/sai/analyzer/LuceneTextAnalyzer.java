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

package org.apache.cassandra.index.sai.analyzer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.BytesRef;

import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.serializers.MarshalException;

/**
 * Materializing analysis service around one shared thread-safe Lucene {@link Analyzer}. Each
 * {@link #analyze} call runs a whole token stream inside the method and returns an immutable
 * list, so no stream object or cross call state ever escapes. Any number of threads may call
 * {@link #analyze} concurrently on one instance. The owner must not race {@link #close()} with
 * in-flight calls, matching how the existing analyzer factory is closed only after the index has
 * left the query path.
 */
public final class LuceneTextAnalyzer implements Closeable
{
    private static final String FIELD_NAME = "sai";

    private final Analyzer analyzer;
    @Nullable
    private final IndexTermType indexTermType;
    private final boolean transformsValue;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Creates an analyzer for string values only, as used by the sai_analyze function. The
     * {@link ByteBuffer} overload of analyze needs the other constructor.
     */
    public LuceneTextAnalyzer(AnalyzerConfig config)
    {
        this(config, null);
    }

    public LuceneTextAnalyzer(AnalyzerConfig config, IndexTermType indexTermType)
    {
        this.analyzer = config.buildAnalyzer();
        this.indexTermType = indexTermType;
        this.transformsValue = config.transformsValue();
    }

    /**
     * Analyzes one column value. A null or zero length value yields an empty list, matching the
     * write path's skip of empty cells.
     *
     * @return every token occurrence in emission order, duplicates included, as an immutable list
     * @throws MarshalException when the value bytes fail decoding, so an unindexable value never
     * passes silently
     */
    public List<AnalyzedToken> analyze(ByteBuffer value)
    {
        if (indexTermType == null)
            throw new IllegalStateException("Analyzing a ByteBuffer value requires the constructor taking an IndexTermType");
        if (value == null || value.remaining() == 0)
            return Collections.emptyList();
        return analyze(indexTermType.asString(value));
    }

    /**
     * Analyzes one string. Unlike the {@link ByteBuffer} overload an empty string runs through
     * the analyzer, so the keyword analyzer emits its single empty token.
     */
    public List<AnalyzedToken> analyze(String value)
    {
        if (value == null)
            return Collections.emptyList();

        List<AnalyzedToken> tokens = new ArrayList<>();
        try (TokenStream stream = analyzer.tokenStream(FIELD_NAME, value))
        {
            TermToBytesRefAttribute termAttribute = stream.addAttribute(TermToBytesRefAttribute.class);
            PositionIncrementAttribute incrementAttribute = stream.addAttribute(PositionIncrementAttribute.class);
            stream.reset();
            int position = -1;
            while (stream.incrementToken())
            {
                position += incrementAttribute.getPositionIncrement();
                tokens.add(new AnalyzedToken(copyOf(termAttribute.getBytesRef()), position));
            }
            stream.end();
        }
        catch (IOException e)
        {
            // in memory input cannot fail IO, and a partial token list must never be returned
            throw new RuntimeException(e);
        }
        return Collections.unmodifiableList(tokens);
    }

    /**
     * @return false only when this analyzer emits the value unchanged
     */
    public boolean transformsValue()
    {
        return transformsValue;
    }

    @Override
    public void close()
    {
        if (closed.compareAndSet(false, true))
            analyzer.close();
    }

    /**
     * Lucene reuses the attribute's backing array across incrementToken calls, so each token's
     * bytes are copied out. For string types the term bytes are UTF-8, which is the column's
     * stored form.
     */
    private static ByteBuffer copyOf(BytesRef bytes)
    {
        ByteBuffer copy = ByteBuffer.allocate(bytes.length);
        copy.put(bytes.bytes, bytes.offset, bytes.length);
        copy.flip();
        return copy;
    }
}
