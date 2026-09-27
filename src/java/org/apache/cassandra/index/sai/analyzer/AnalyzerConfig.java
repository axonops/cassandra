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

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import org.apache.lucene.analysis.Analyzer;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.utils.IndexTermType;

/**
 * One parsed and validated analyzer definition from the {@code index_analyzer} or
 * {@code query_analyzer} index option. The value is either a {@link BuiltInAnalyzer} name or a
 * JSON object naming a tokenizer, char filters and token filters resolved through Lucene's SPI
 * factory registries. Validation is eager: a config that parses has already assembled its Lucene
 * {@link Analyzer} once, so a bad factory argument fails at DDL time and not on the first write.
 */
public final class AnalyzerConfig
{
    public static final String INDEX_ANALYZER_OPTION = "index_analyzer";
    public static final String QUERY_ANALYZER_OPTION = "query_analyzer";

    /** Index options live in schema and gossip, so both caps bound schema bloat, not function */
    public static final int MAX_CONFIG_LENGTH = 8192;
    public static final int MAX_COMPONENTS = 32;

    private final String optionName;
    @Nullable
    private final BuiltInAnalyzer builtIn;
    @Nullable
    private final Component tokenizer;
    private final List<Component> charFilters;
    private final List<Component> filters;

    private AnalyzerConfig(String optionName, BuiltInAnalyzer builtIn)
    {
        this.optionName = optionName;
        this.builtIn = builtIn;
        this.tokenizer = null;
        this.charFilters = List.of();
        this.filters = List.of();
    }

    AnalyzerConfig(String optionName, Component tokenizer, List<Component> charFilters, List<Component> filters)
    {
        this.optionName = optionName;
        this.builtIn = null;
        this.tokenizer = tokenizer;
        this.charFilters = charFilters;
        this.filters = filters;
    }

    /**
     * Parses and validates one analyzer option value. The trimmed value starting with a JSON
     * bracket is parsed as a custom analyzer document, anything else is looked up as a built-in
     * analyzer name.
     *
     * @throws InvalidRequestException when the value breaks any validation rule
     */
    public static AnalyzerConfig parse(String optionName, String value)
    {
        if (value == null || value.trim().isEmpty())
            throw rejected("Analyzer option '%s' cannot be empty", optionName);

        if (value.length() > MAX_CONFIG_LENGTH)
            throw rejected("Analyzer configuration is longer than %s characters", MAX_CONFIG_LENGTH);

        String trimmed = value.trim();
        if (trimmed.startsWith("{") || trimmed.startsWith("["))
        {
            AnalyzerConfig config = AnalyzerConfigParser.parseJson(optionName, trimmed);
            config.buildAnalyzer().close();
            return config;
        }

        BuiltInAnalyzer builtIn = BuiltInAnalyzer.fromName(trimmed);
        if (builtIn == null)
            throw rejected("Unknown analyzer '%s'. Valid built-in analyzers: %s. A custom analyzer must be a JSON object.",
                           trimmed, BuiltInAnalyzer.validNames());
        return new AnalyzerConfig(optionName, builtIn);
    }

    /**
     * Applies the map level rules to a full index options map and parses the analyzer options it
     * contains. The map is only read, never mutated.
     *
     * @return the parsed index and query analyzer configs, or null when the map configures no
     * Lucene analyzer
     * @throws InvalidRequestException when the options break any validation rule
     */
    public static Configs fromIndexOptions(IndexTermType indexTermType, Map<String, String> options)
    {
        boolean hasIndexAnalyzer = options.containsKey(INDEX_ANALYZER_OPTION);
        boolean hasQueryAnalyzer = options.containsKey(QUERY_ANALYZER_OPTION);

        if (!hasIndexAnalyzer && !hasQueryAnalyzer)
            return null;

        String analyzerOption = hasIndexAnalyzer ? INDEX_ANALYZER_OPTION : QUERY_ANALYZER_OPTION;
        for (String option : options.keySet())
        {
            if (NonTokenizingOptions.hasOption(option))
                throw rejected("Option '%s' cannot be combined with '%s'", analyzerOption, option);
        }

        if (!hasIndexAnalyzer)
            throw rejected("Option '%s' requires '%s'", QUERY_ANALYZER_OPTION, INDEX_ANALYZER_OPTION);

        if (!indexTermType.isString())
            throw rejected("CQL type %s cannot be analyzed", indexTermType.asCQL3Type());

        AnalyzerConfig indexConfig = parse(INDEX_ANALYZER_OPTION, options.get(INDEX_ANALYZER_OPTION));
        AnalyzerConfig queryConfig = hasQueryAnalyzer
                                     ? parse(QUERY_ANALYZER_OPTION, options.get(QUERY_ANALYZER_OPTION))
                                     : null;
        return new Configs(indexConfig, queryConfig);
    }

    /**
     * @return a new Lucene {@link Analyzer} for this config, owned by the caller
     */
    public Analyzer buildAnalyzer()
    {
        return builtIn != null ? builtIn.newAnalyzer() : AnalyzerConfigParser.build(tokenizer, charFilters, filters);
    }

    /**
     * @return false only for the keyword built-in, which emits the value unchanged, true for
     * every analyzer that may rewrite it
     */
    public boolean transformsValue()
    {
        return builtIn != BuiltInAnalyzer.KEYWORD;
    }

    public String optionName()
    {
        return optionName;
    }

    @Nullable
    BuiltInAnalyzer builtIn()
    {
        return builtIn;
    }

    /**
     * Rejects an analyzer config, counting the rejection before it surfaces to the client.
     */
    static InvalidRequestException rejected(String message, Object... args)
    {
        AnalyzedTermLimits.countAnalyzerConfigError();
        return new InvalidRequestException(String.format(message, args));
    }

    /**
     * The index and query analyzer configs selected by one index options map. The query config is
     * null when {@code query_analyzer} is absent, in which case the query path uses the index
     * analyzer instance.
     */
    public static final class Configs
    {
        public final AnalyzerConfig indexConfig;
        @Nullable
        public final AnalyzerConfig queryConfig;

        Configs(AnalyzerConfig indexConfig, AnalyzerConfig queryConfig)
        {
            this.indexConfig = indexConfig;
            this.queryConfig = queryConfig;
        }
    }

    /**
     * One named component of a custom analyzer, a tokenizer, char filter or token filter, with
     * its factory arguments.
     */
    static final class Component
    {
        final String name;
        final Map<String, String> args;

        Component(String name, Map<String, String> args)
        {
            this.name = name;
            this.args = args;
        }
    }
}
