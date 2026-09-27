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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public class BuiltInAnalyzerTest
{
    /**
     * The enforcement gate for the shipped registry: every constant must construct against the
     * Lucene jars in lib and analyze a probe string without error.
     */
    @Test
    public void everyBuiltInConstructsAndAnalyzes()
    {
        for (BuiltInAnalyzer builtIn : BuiltInAnalyzer.values())
        {
            String name = builtIn.name().toLowerCase(Locale.ROOT);
            assertSame(builtIn, BuiltInAnalyzer.fromName(name));
            assertNotNull(name, analyze(name, "The quick brown fox 123"));
        }
    }

    @Test
    public void standardTokenizes()
    {
        assertEquals(List.of("the", "quick", "brown", "fox"), analyze("standard", "The Quick Brown Fox"));
    }

    @Test
    public void keywordEmitsValueUnchanged()
    {
        assertEquals(List.of("Hello World"), analyze("keyword", "Hello World"));
    }

    @Test
    public void lowercaseIsCaseInsensitiveKeyword()
    {
        assertEquals(List.of("hello world"), analyze("lowercase", "Hello World"));
    }

    @Test
    public void stopRemovesEnglishStopwords()
    {
        assertEquals(List.of("quick", "fox"), analyze("stop", "The Quick the Fox"));
    }

    @Test
    public void whitespaceSplitsOnWhitespaceOnly()
    {
        assertEquals(List.of("Hello,World", "foo"), analyze("whitespace", "Hello,World foo"));
    }

    @Test
    public void simpleSplitsOnNonLetters()
    {
        assertEquals(List.of("foo", "bar", "baz"), analyze("simple", "foo123bar Baz"));
    }

    @Test
    public void englishStems()
    {
        assertEquals(List.of("run", "run", "ran"), analyze("english", "running runs ran"));
    }

    @Test
    public void germanStems()
    {
        assertEquals(List.of("haus", "haus"), analyze("german", "Häuser Haus"));
    }

    @Test
    public void frenchStems()
    {
        assertEquals(List.of("cheval", "cheval"), analyze("french", "cheval chevaux"));
    }

    @Test
    public void thaiTokenizes()
    {
        assertEquals(List.of("สวัสดี", "ครับ"), analyze("thai", "สวัสดีครับ"));
    }

    private List<String> analyze(String builtInName, String text)
    {
        AnalyzerConfig config = AnalyzerConfig.parse(AnalyzerConfig.INDEX_ANALYZER_OPTION, builtInName);
        try (LuceneTextAnalyzer analyzer = new LuceneTextAnalyzer(config))
        {
            List<String> tokens = new ArrayList<>();
            for (AnalyzedToken token : analyzer.analyze(text))
                tokens.add(UTF8Type.instance.compose(token.bytes()));
            return tokens;
        }
    }
}
