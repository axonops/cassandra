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

package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.analyzer.AnalyzedToken;
import org.apache.cassandra.index.sai.analyzer.AnalyzerConfig;
import org.apache.cassandra.index.sai.analyzer.LuceneTextAnalyzer;

/**
 * Debug functions for SAI text analysis. {@code sai_analyze(value, analyzer_config)} accepts
 * exactly the value space of the {@code index_analyzer} index option, with the same validation
 * and the same error messages, so users can validate a config and preview its tokenization
 * before creating an index. Each returned element is one token occurrence in emission order,
 * duplicates included, in the form token@position, where the position is zero based with gaps
 * left by filters that remove tokens preserved.
 */
public final class SaiFunctions
{
    public static final String ANALYZE_FUNCTION_NAME = "sai_analyze";

    static final String ANALYZER_CONFIG_ARGUMENT_NAME = "analyzer_config";

    private static final ListType<String> RETURN_TYPE = ListType.getInstance(UTF8Type.instance, false);

    private SaiFunctions()
    {
    }

    public static void addFunctionsTo(NativeFunctions functions)
    {
        functions.add(new NativeScalarFunction(ANALYZE_FUNCTION_NAME, RETURN_TYPE, UTF8Type.instance, UTF8Type.instance)
        {
            @Override
            public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
            {
                if (arguments.containsNulls())
                    return null;

                String value = arguments.get(0);
                String config = arguments.get(1);

                AnalyzerConfig analyzerConfig = AnalyzerConfig.parse(ANALYZER_CONFIG_ARGUMENT_NAME, config);
                try (LuceneTextAnalyzer analyzer = new LuceneTextAnalyzer(analyzerConfig))
                {
                    List<String> tokens = new ArrayList<>();
                    for (AnalyzedToken token : analyzer.analyze(value))
                        tokens.add(UTF8Type.instance.compose(token.bytes()) + '@' + token.position());
                    return RETURN_TYPE.decompose(tokens);
                }
            }
        });
    }
}
