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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharFilterFactory;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenizerFactory;
import org.apache.lucene.analysis.custom.CustomAnalyzer;

import org.apache.cassandra.index.sai.analyzer.AnalyzerConfig.Component;
import org.apache.cassandra.utils.JsonUtils;

import static org.apache.cassandra.index.sai.analyzer.AnalyzerConfig.rejected;

/**
 * The JSON walker behind {@link AnalyzerConfig#parse}. The schema is two levels deep: an object
 * with the keys {@code tokenizer}, {@code charFilters} and {@code filters}, whose entries are
 * objects with a string {@code name} and an optional {@code args} object of scalar values.
 * Anything outside that shape is rejected, so nested JSON never reaches Lucene factory code.
 * Component names resolve only through Lucene's SPI factory registries and the assembled
 * analyzer only ever sees a {@link RejectingResourceLoader}, so no option value can load a class
 * or a resource file.
 */
final class AnalyzerConfigParser
{
    static final String TOKENIZER_KEY = "tokenizer";
    static final String FILTERS_KEY = "filters";
    static final String CHAR_FILTERS_KEY = "charFilters";
    static final String NAME_KEY = "name";
    static final String ARGS_KEY = "args";

    static final String TOKENIZER_KIND = "tokenizer";
    static final String FILTER_KIND = "filter";
    static final String CHAR_FILTER_KIND = "charFilter";

    /** The tokenizer used when the document names none, so a filters-only config is legal */
    static final String DEFAULT_TOKENIZER = "keyword";

    private AnalyzerConfigParser()
    {
    }

    static AnalyzerConfig parseJson(String optionName, String json)
    {
        JsonNode root;
        try
        {
            root = JsonUtils.JSON_OBJECT_MAPPER.readTree(json);
        }
        catch (IOException e)
        {
            throw rejected("Invalid analyzer JSON for option '%s': %s", optionName, e.getMessage());
        }

        if (!root.isObject())
            throw rejected("Analyzer JSON must be an object with keys 'tokenizer', 'filters' and 'charFilters'");

        for (Iterator<String> fields = root.fieldNames(); fields.hasNext();)
        {
            String field = fields.next();
            if (!field.equals(TOKENIZER_KEY) && !field.equals(FILTERS_KEY) && !field.equals(CHAR_FILTERS_KEY))
                throw rejected("Unknown analyzer JSON key '%s'. Supported keys: tokenizer, filters, charFilters", field);
        }

        Component tokenizer = root.has(TOKENIZER_KEY)
                              ? parseComponent(root.get(TOKENIZER_KEY), TOKENIZER_KIND)
                              : new Component(DEFAULT_TOKENIZER, Map.of());
        List<Component> charFilters = parseComponents(root.get(CHAR_FILTERS_KEY), CHAR_FILTER_KIND);
        List<Component> filters = parseComponents(root.get(FILTERS_KEY), FILTER_KIND);

        if (charFilters.size() + filters.size() > AnalyzerConfig.MAX_COMPONENTS)
            throw rejected("Analyzer configuration has more than %s filters", AnalyzerConfig.MAX_COMPONENTS);

        checkName(TOKENIZER_KIND, tokenizer.name, TokenizerFactory.availableTokenizers());
        for (Component charFilter : charFilters)
            checkName(CHAR_FILTER_KIND, charFilter.name, CharFilterFactory.availableCharFilters());
        for (Component filter : filters)
            checkName(FILTER_KIND, filter.name, TokenFilterFactory.availableTokenFilters());

        return new AnalyzerConfig(optionName, tokenizer, charFilters, filters);
    }

    static Analyzer build(Component tokenizer, List<Component> charFilters, List<Component> filters)
    {
        try
        {
            CustomAnalyzer.Builder builder = CustomAnalyzer.builder(new RejectingResourceLoader());
            try
            {
                builder.withTokenizer(tokenizer.name, new HashMap<>(tokenizer.args));
            }
            catch (IllegalArgumentException e)
            {
                throw rejected("Invalid configuration for %s '%s': %s", TOKENIZER_KIND, tokenizer.name, e.getMessage());
            }
            for (Component charFilter : charFilters)
            {
                try
                {
                    builder.addCharFilter(charFilter.name, new HashMap<>(charFilter.args));
                }
                catch (IllegalArgumentException e)
                {
                    throw rejected("Invalid configuration for %s '%s': %s", CHAR_FILTER_KIND, charFilter.name, e.getMessage());
                }
            }
            for (Component filter : filters)
            {
                try
                {
                    builder.addTokenFilter(filter.name, new HashMap<>(filter.args));
                }
                catch (IllegalArgumentException e)
                {
                    throw rejected("Invalid configuration for %s '%s': %s", FILTER_KIND, filter.name, e.getMessage());
                }
            }
            return builder.build();
        }
        catch (IOException e)
        {
            // the components hold in memory input and a loader that never opens anything
            throw new RuntimeException(e);
        }
    }

    private static List<Component> parseComponents(JsonNode node, String kind)
    {
        if (node == null)
            return List.of();
        if (!node.isArray())
            throw badComponent();

        List<Component> components = new ArrayList<>(node.size());
        for (JsonNode entry : node)
            components.add(parseComponent(entry, kind));
        return components;
    }

    private static Component parseComponent(JsonNode node, String kind)
    {
        if (!node.isObject())
            throw badComponent();

        for (Iterator<String> fields = node.fieldNames(); fields.hasNext();)
        {
            String field = fields.next();
            if (!field.equals(NAME_KEY) && !field.equals(ARGS_KEY))
                throw badComponent();
        }

        JsonNode name = node.get(NAME_KEY);
        if (name == null || !name.isTextual())
            throw badComponent();

        Map<String, String> args = new LinkedHashMap<>();
        JsonNode argsNode = node.get(ARGS_KEY);
        if (argsNode != null)
        {
            if (!argsNode.isObject())
                throw badComponent();

            for (Iterator<Map.Entry<String, JsonNode>> entries = argsNode.fields(); entries.hasNext();)
            {
                Map.Entry<String, JsonNode> entry = entries.next();
                JsonNode value = entry.getValue();
                if (!value.isTextual() && !value.isNumber() && !value.isBoolean())
                    throw rejected("Argument '%s' of %s '%s' must be a string, number or boolean",
                                   entry.getKey(), kind, name.asText());
                args.put(entry.getKey(), value.asText());
            }
        }
        return new Component(name.asText(), args);
    }

    private static RuntimeException badComponent()
    {
        return rejected("Analyzer component must be an object with a string 'name' and optional 'args' object");
    }

    /**
     * Pre-checks the component name against the SPI registry so the error message and its
     * allow-list are ours. Lucene registers factory names case insensitively and so does this
     * check.
     */
    private static void checkName(String kind, String name, Set<String> available)
    {
        for (String candidate : available)
        {
            if (candidate.equalsIgnoreCase(name))
                return;
        }
        throw rejected("Unknown %s '%s'. Available: %s", kind, name, new TreeSet<>(available));
    }
}
