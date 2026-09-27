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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;

import static java.util.Collections.unmodifiableMap;
import static org.apache.cassandra.index.sai.analyzer.AnalyzerConfig.INDEX_ANALYZER_OPTION;
import static org.apache.cassandra.index.sai.analyzer.AnalyzerConfig.QUERY_ANALYZER_OPTION;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class AnalyzerConfigTest
{
    @Test
    public void unknownBuiltInNameFails()
    {
        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION, "bogus"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageStartingWith("Unknown analyzer 'bogus'. Valid built-in analyzers: ")
        .hasMessageContaining("standard")
        .hasMessageEndingWith("A custom analyzer must be a JSON object.");
    }

    @Test
    public void builtInNameIsCaseInsensitive()
    {
        assertSame(BuiltInAnalyzer.STANDARD, AnalyzerConfig.parse(INDEX_ANALYZER_OPTION, "STANDARD").builtIn());
        assertSame(BuiltInAnalyzer.STANDARD, AnalyzerConfig.parse(INDEX_ANALYZER_OPTION, "Standard").builtIn());
        assertSame(BuiltInAnalyzer.ENGLISH, AnalyzerConfig.parse(INDEX_ANALYZER_OPTION, "English").builtIn());
    }

    @Test
    public void unknownTokenizerFails()
    {
        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION, "{\"tokenizer\": {\"name\": \"bogus\"}}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageStartingWith("Unknown tokenizer 'bogus'. Available: ")
        .hasMessageContaining("standard");
    }

    @Test
    public void unknownFilterFails()
    {
        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION, "{\"filters\": [{\"name\": \"bogus\"}]}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageStartingWith("Unknown filter 'bogus'. Available: ")
        .hasMessageContaining("lowercase");
    }

    @Test
    public void unknownCharFilterFails()
    {
        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION, "{\"charFilters\": [{\"name\": \"bogus\"}]}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageStartingWith("Unknown charFilter 'bogus'. Available: ")
        .hasMessageContaining("htmlStrip");
    }

    @Test
    public void malformedJsonFails()
    {
        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION, "{\"tokenizer\": "))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageStartingWith("Invalid analyzer JSON for option 'index_analyzer': ");
    }

    @Test
    public void topLevelArrayFails()
    {
        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION, "[{\"name\": \"standard\"}]"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Analyzer JSON must be an object with keys 'tokenizer', 'filters' and 'charFilters'");
    }

    @Test
    public void unknownTopLevelKeyFails()
    {
        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION, "{\"tokeniser\": {\"name\": \"standard\"}}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Unknown analyzer JSON key 'tokeniser'. Supported keys: tokenizer, filters, charFilters");
    }

    @Test
    public void componentMissingNameFails()
    {
        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION, "{\"tokenizer\": {\"args\": {}}}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Analyzer component must be an object with a string 'name' and optional 'args' object");
    }

    @Test
    public void componentWrongShapeFails()
    {
        String expected = "Analyzer component must be an object with a string 'name' and optional 'args' object";

        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION, "{\"tokenizer\": \"standard\"}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage(expected);

        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION, "{\"filters\": {\"name\": \"stop\"}}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage(expected);

        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION,
                                                      "{\"tokenizer\": {\"name\": \"standard\", \"extra\": 1}}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage(expected);

        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION, "{\"tokenizer\": {\"name\": 1}}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage(expected);
    }

    @Test
    public void oversizeConfigFails()
    {
        String oversize = '{' + " ".repeat(AnalyzerConfig.MAX_CONFIG_LENGTH) + '}';
        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION, oversize))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Analyzer configuration is longer than 8192 characters");
    }

    @Test
    public void tooManyFiltersFails()
    {
        StringBuilder config = new StringBuilder("{\"filters\": [");
        for (int i = 0; i <= AnalyzerConfig.MAX_COMPONENTS; i++)
        {
            if (i > 0)
                config.append(", ");
            config.append("{\"name\": \"lowercase\"}");
        }
        config.append("]}");

        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION, config.toString()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Analyzer configuration has more than 32 filters");
    }

    @Test
    public void nestedArgObjectFails()
    {
        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION,
                                                      "{\"tokenizer\": {\"name\": \"standard\", \"args\": {\"a\": {\"b\": 1}}}}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Argument 'a' of tokenizer 'standard' must be a string, number or boolean");

        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION,
                                                      "{\"filters\": [{\"name\": \"stop\", \"args\": {\"words\": [\"a\"]}}]}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Argument 'words' of filter 'stop' must be a string, number or boolean");

        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION,
                                                      "{\"filters\": [{\"name\": \"stop\", \"args\": {\"words\": null}}]}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Argument 'words' of filter 'stop' must be a string, number or boolean");
    }

    @Test
    public void deeplyNestedJsonFails()
    {
        int depth = 200;
        StringBuilder nested = new StringBuilder("{\"tokenizer\": {\"name\": \"standard\", \"args\": {\"a\": ");
        for (int i = 0; i < depth; i++)
            nested.append("{\"b\": ");
        nested.append('1');
        for (int i = 0; i < depth; i++)
            nested.append('}');
        nested.append("}}}");

        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION, nested.toString()))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Argument 'a' of tokenizer 'standard' must be a string, number or boolean");
    }

    @Test
    public void filtersOnlyDefaultsToKeywordTokenizer()
    {
        assertEquals(List.of("hello world"), analyze("{\"filters\": [{\"name\": \"lowercase\"}]}", "Hello World"));
    }

    @Test
    public void emptyJsonObjectIsKeywordAnalyzer()
    {
        assertEquals(List.of("Hello World"), analyze("{}", "Hello World"));
    }

    @Test
    public void synonymAnalyzerClassArgFails()
    {
        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION,
                                                      "{\"filters\": [{\"name\": \"synonymGraph\", " +
                                                      "\"args\": {\"synonyms\": \"syn.txt\", \"analyzer\": \"evil.Clazz\"}}]}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageStartingWith("Analyzer options must not load classes (requested '");
    }

    @Test
    public void resourceFileArgFails()
    {
        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION,
                                                      "{\"filters\": [{\"name\": \"stop\", \"args\": {\"words\": \"stop.txt\"}}]}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Analyzer options must not load resource files (requested 'stop.txt')");
    }

    @Test
    public void stopFilterWithDefaultSetWorks()
    {
        assertEquals(List.of("fox", "hound"),
                     analyze("{\"tokenizer\": {\"name\": \"whitespace\"}, \"filters\": [{\"name\": \"stop\"}]}",
                             "the fox and hound"));
    }

    @Test
    public void emptyOptionValueFails()
    {
        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION, ""))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Analyzer option 'index_analyzer' cannot be empty");

        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION, "   "))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Analyzer option 'index_analyzer' cannot be empty");

        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION, null))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Analyzer option 'index_analyzer' cannot be empty");
    }

    @Test
    public void nonStringTypeFails()
    {
        Map<String, String> options = Map.of(INDEX_ANALYZER_OPTION, "standard");
        assertThatThrownBy(() -> AnalyzerConfig.fromIndexOptions(SAITester.createIndexTermType(Int32Type.instance), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("CQL type int cannot be analyzed");
    }

    @Test
    public void queryAnalyzerWithoutIndexAnalyzerFails()
    {
        Map<String, String> options = Map.of(QUERY_ANALYZER_OPTION, "standard");
        assertThatThrownBy(() -> AnalyzerConfig.fromIndexOptions(SAITester.createIndexTermType(UTF8Type.instance), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Option 'query_analyzer' requires 'index_analyzer'");
    }

    @Test
    public void conflictWithNonTokenizingOptionsFails()
    {
        Map<String, String> options = Map.of(INDEX_ANALYZER_OPTION, "standard",
                                             NonTokenizingOptions.CASE_SENSITIVE, "false");
        assertThatThrownBy(() -> AnalyzerConfig.fromIndexOptions(SAITester.createIndexTermType(UTF8Type.instance), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Option 'index_analyzer' cannot be combined with 'case_sensitive'");

        Map<String, String> queryOptions = Map.of(QUERY_ANALYZER_OPTION, "standard",
                                                  NonTokenizingOptions.NORMALIZE, "true");
        assertThatThrownBy(() -> AnalyzerConfig.fromIndexOptions(SAITester.createIndexTermType(UTF8Type.instance), queryOptions))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Option 'query_analyzer' cannot be combined with 'normalize'");
    }

    @Test
    public void optionsMapNotMutated()
    {
        Map<String, String> options = new HashMap<>();
        options.put(INDEX_ANALYZER_OPTION, "standard");
        options.put(QUERY_ANALYZER_OPTION, "{\"filters\": [{\"name\": \"lowercase\"}]}");
        Map<String, String> original = new HashMap<>(options);

        AnalyzerConfig.Configs configs = AnalyzerConfig.fromIndexOptions(SAITester.createIndexTermType(UTF8Type.instance),
                                                                         unmodifiableMap(options));

        assertNotNull(configs.indexConfig);
        assertNotNull(configs.queryConfig);
        assertEquals(original, options);
    }

    @Test
    public void fromIndexOptionsWithoutAnalyzerOptionsReturnsNull()
    {
        Map<String, String> options = Map.of(NonTokenizingOptions.CASE_SENSITIVE, "false");
        assertNull(AnalyzerConfig.fromIndexOptions(SAITester.createIndexTermType(UTF8Type.instance), options));
    }

    @Test
    public void factoryArgsApplied()
    {
        assertEquals(List.of("ab", "bc", "cd"),
                     analyze("{\"tokenizer\": {\"name\": \"nGram\", \"args\": {\"minGramSize\": 2, \"maxGramSize\": 2}}}",
                             "abcd"));
    }

    @Test
    public void invalidFactoryArgsFail()
    {
        assertThatThrownBy(() -> AnalyzerConfig.parse(INDEX_ANALYZER_OPTION,
                                                      "{\"tokenizer\": {\"name\": \"standard\", \"args\": {\"bogusArg\": \"1\"}}}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageStartingWith("Invalid configuration for tokenizer 'standard': ");
    }

    private List<String> analyze(String config, String text)
    {
        AnalyzerConfig analyzerConfig = AnalyzerConfig.parse(INDEX_ANALYZER_OPTION, config);
        try (LuceneTextAnalyzer analyzer = new LuceneTextAnalyzer(analyzerConfig))
        {
            List<String> tokens = new ArrayList<>();
            for (AnalyzedToken token : analyzer.analyze(text))
                tokens.add(UTF8Type.instance.compose(token.bytes()));
            return tokens;
        }
    }
}
