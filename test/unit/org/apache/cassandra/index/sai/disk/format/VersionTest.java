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
package org.apache.cassandra.index.sai.disk.format;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.disk.v2.V2OnDiskFormat;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VersionTest
{
    @BeforeClass
    public static void initialise()
    {
        DatabaseDescriptor.toolInitialization();
    }

    @Test
    public void supportedVersionsWillParse()
    {
        assertEquals(Version.AA, Version.parse("aa"));
        assertEquals(Version.AB, Version.parse("ab"));
    }

    @Test
    public void unsupportedOrInvalidVersionsDoNotParse()
    {
        assertThatThrownBy(() -> Version.parse(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Version.parse("zz")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Version.parse("a")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Version.parse("abc")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void analyzedVersionOrdersAfterStockVersion()
    {
        assertTrue(Version.AB.onOrAfter(Version.AA));
        assertFalse(Version.AA.onOrAfter(Version.AB));
    }

    @Test
    public void latestVersionStaysStock()
    {
        // Only indexes with an index_analyzer write version ab, everything else keeps the default
        assertEquals(Version.AA, Version.LATEST);
        assertEquals(V2OnDiskFormat.instance, Version.AB.onDiskFormat());
    }
}
