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

import java.io.InputStream;

import org.apache.lucene.util.ResourceLoader;

/**
 * The {@link ResourceLoader} handed to Lucene when assembling a custom analyzer. Analyzer options
 * come from user supplied schema, so every request to load a class or resource file is rejected.
 * Configs whose factories never ask the loader for anything are unaffected.
 */
final class RejectingResourceLoader implements ResourceLoader
{
    @Override
    public InputStream openResource(String resource)
    {
        throw AnalyzerConfig.rejected("Analyzer options must not load resource files (requested '%s')", resource);
    }

    @Override
    public <T> Class<? extends T> findClass(String cname, Class<T> expectedType)
    {
        throw AnalyzerConfig.rejected("Analyzer options must not load classes (requested '%s')", cname);
    }

    @Override
    public <T> T newInstance(String cname, Class<T> expectedType)
    {
        throw AnalyzerConfig.rejected("Analyzer options must not load classes (requested '%s')", cname);
    }
}
