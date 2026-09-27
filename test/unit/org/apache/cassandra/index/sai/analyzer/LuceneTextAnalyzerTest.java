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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LuceneTextAnalyzerTest
{
    private static final String STANDARD_WITH_STOP = "{\"tokenizer\": {\"name\": \"standard\"}, " +
                                                     "\"filters\": [{\"name\": \"lowercase\"}, {\"name\": \"stop\"}]}";

    @Test
    public void positionsAreZeroBasedAndOrdered()
    {
        try (LuceneTextAnalyzer analyzer = analyzer("standard"))
        {
            List<AnalyzedToken> tokens = analyzer.analyze("one two three");
            assertEquals(3, tokens.size());
            assertEquals("one", string(tokens.get(0)));
            assertEquals(0, tokens.get(0).position());
            assertEquals("two", string(tokens.get(1)));
            assertEquals(1, tokens.get(1).position());
            assertEquals("three", string(tokens.get(2)));
            assertEquals(2, tokens.get(2).position());
        }
    }

    @Test
    public void stopwordGapPreservesPositions()
    {
        try (LuceneTextAnalyzer analyzer = analyzer(STANDARD_WITH_STOP))
        {
            List<AnalyzedToken> tokens = analyzer.analyze("quick the fox");
            assertEquals(2, tokens.size());
            assertEquals("quick", string(tokens.get(0)));
            assertEquals(0, tokens.get(0).position());
            assertEquals("fox", string(tokens.get(1)));
            assertEquals(2, tokens.get(1).position());
        }
    }

    @Test
    public void duplicateTokensKeepPositions()
    {
        try (LuceneTextAnalyzer analyzer = analyzer("standard"))
        {
            List<AnalyzedToken> tokens = analyzer.analyze("fox fox fox");
            assertEquals(3, tokens.size());
            for (int i = 0; i < tokens.size(); i++)
            {
                assertEquals("fox", string(tokens.get(i)));
                assertEquals(i, tokens.get(i).position());
            }
        }
    }

    @Test
    public void repeatedAnalyzeCallsAreIndependent()
    {
        try (LuceneTextAnalyzer analyzer = analyzer("standard"))
        {
            List<AnalyzedToken> first = analyzer.analyze("quick brown fox");
            List<AnalyzedToken> second = analyzer.analyze("quick brown fox");
            assertEquals(first, second);
        }
    }

    @Test
    public void returnedTokensSurviveNextCall()
    {
        try (LuceneTextAnalyzer analyzer = analyzer("standard"))
        {
            List<AnalyzedToken> first = analyzer.analyze("quick brown fox");
            analyzer.analyze("completely different words here");
            assertEquals("quick", string(first.get(0)));
            assertEquals("brown", string(first.get(1)));
            assertEquals("fox", string(first.get(2)));
        }
    }

    @Test
    public void emptyValueYieldsEmptyList()
    {
        try (LuceneTextAnalyzer analyzer = analyzer("standard"))
        {
            assertTrue(analyzer.analyze((ByteBuffer) null).isEmpty());
            assertTrue(analyzer.analyze(ByteBufferUtil.EMPTY_BYTE_BUFFER).isEmpty());
            assertTrue(analyzer.analyze((String) null).isEmpty());
        }
    }

    @Test
    public void allStopwordValueYieldsEmptyList()
    {
        try (LuceneTextAnalyzer analyzer = analyzer(STANDARD_WITH_STOP))
        {
            assertTrue(analyzer.analyze("the and of").isEmpty());
        }
    }

    @Test
    public void undecodableInputThrowsMarshalException()
    {
        try (LuceneTextAnalyzer analyzer = analyzer("standard"))
        {
            ByteBuffer invalidUtf8 = ByteBuffer.wrap(new byte[]{ (byte) 0xC3, (byte) 0x28 });
            assertThatThrownBy(() -> analyzer.analyze(invalidUtf8)).isInstanceOf(MarshalException.class);
        }
    }

    @Test
    public void closeIsIdempotent()
    {
        LuceneTextAnalyzer analyzer = analyzer("standard");
        analyzer.close();
        analyzer.close();
    }

    @Test
    public void concurrentAnalyzeProducesIdenticalResults() throws Exception
    {
        int threads = 4;
        int iterations = 100;
        String text = "The quick brown fox jumps over the lazy dog";

        try (LuceneTextAnalyzer analyzer = analyzer("standard"))
        {
            List<AnalyzedToken> expected = analyzer.analyze(text);

            ExecutorService executor = Executors.newFixedThreadPool(threads);
            try
            {
                List<Future<Boolean>> results = new ArrayList<>();
                for (int i = 0; i < threads; i++)
                {
                    results.add(executor.submit(() -> {
                        for (int j = 0; j < iterations; j++)
                        {
                            if (!expected.equals(analyzer.analyze(text)))
                                return false;
                        }
                        return true;
                    }));
                }
                for (Future<Boolean> result : results)
                    assertTrue(result.get(1, TimeUnit.MINUTES));
            }
            finally
            {
                executor.shutdown();
                assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
            }
        }
    }

    private static LuceneTextAnalyzer analyzer(String config)
    {
        return new LuceneTextAnalyzer(AnalyzerConfig.parse(AnalyzerConfig.INDEX_ANALYZER_OPTION, config),
                                      SAITester.createIndexTermType(UTF8Type.instance));
    }

    private static String string(AnalyzedToken token)
    {
        return UTF8Type.instance.compose(token.bytes());
    }
}
