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

import java.util.Map;

/**
 * The {@code equals_behaviour_when_analyzed} index option: what an {@code =} restriction means on a
 * column indexed with an {@code index_analyzer}. Parsed and stored at DDL time, enforced by query
 * planning.
 */
public enum EqualsBehaviourWhenAnalyzed
{
    /** {@code =} behaves like the analyzed match operator */
    MATCH,

    /** {@code =} is rejected on the analyzed column, the default */
    UNSUPPORTED;

    public static final String OPTION = "equals_behaviour_when_analyzed";

    /**
     * Parses the option from a full index options map.
     *
     * @return the configured behaviour, or {@link #UNSUPPORTED} when the option is absent
     * @throws org.apache.cassandra.exceptions.InvalidRequestException when the value is not a legal
     * behaviour or the option appears without {@code index_analyzer}
     */
    public static EqualsBehaviourWhenAnalyzed fromOptions(Map<String, String> options)
    {
        String value = options.get(OPTION);
        if (value == null)
            return UNSUPPORTED;

        if (!options.containsKey(AnalyzerConfig.INDEX_ANALYZER_OPTION))
            throw AnalyzerConfig.rejected("Option '%s' requires '%s'", OPTION, AnalyzerConfig.INDEX_ANALYZER_OPTION);

        for (EqualsBehaviourWhenAnalyzed behaviour : values())
        {
            if (behaviour.name().equalsIgnoreCase(value.trim()))
                return behaviour;
        }
        throw AnalyzerConfig.rejected("Illegal value for option '%s': '%s'. Valid values: %s, %s",
                                      OPTION, value, MATCH, UNSUPPORTED);
    }
}
