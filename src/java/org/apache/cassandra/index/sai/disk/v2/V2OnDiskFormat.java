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

package org.apache.cassandra.index.sai.disk.v2;

import java.util.EnumSet;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat;
import org.apache.cassandra.index.sai.utils.IndexTermType;

/**
 * The on-disk format used by {@link org.apache.cassandra.index.sai.disk.format.Version#AB}. Only
 * indexes with an {@code index_analyzer} option ever use this format, so the literal component set
 * always includes {@link IndexComponent#POSITIONS}. Everything else is inherited unchanged from
 * {@link V1OnDiskFormat}, keeping non-analyzed indexes byte-identical to version aa.
 */
public class V2OnDiskFormat extends V1OnDiskFormat
{
    @VisibleForTesting
    public static final Set<IndexComponent> ANALYZED_LITERAL_COMPONENTS = EnumSet.of(IndexComponent.COLUMN_COMPLETION_MARKER,
                                                                                     IndexComponent.META,
                                                                                     IndexComponent.TERMS_DATA,
                                                                                     IndexComponent.POSTING_LISTS,
                                                                                     IndexComponent.POSITIONS);

    public static final V2OnDiskFormat instance = new V2OnDiskFormat();

    protected V2OnDiskFormat()
    {}

    @Override
    public Set<IndexComponent> perColumnIndexComponents(IndexTermType indexTermType)
    {
        return indexTermType.isLiteral() && !indexTermType.isVector() ? ANALYZED_LITERAL_COMPONENTS
                                                                      : super.perColumnIndexComponents(indexTermType);
    }

    @Override
    public int openFilesPerColumnIndex()
    {
        // terms + postings + positions
        return 3;
    }
}
