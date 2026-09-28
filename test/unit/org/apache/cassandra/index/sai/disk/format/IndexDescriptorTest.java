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

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.disk.v2.V2OnDiskFormat;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.File;
import org.apache.lucene.store.IndexInput;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexDescriptorTest
{
    private final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private Descriptor descriptor;

    @BeforeClass
    public static void initialise()
    {
        DatabaseDescriptor.toolInitialization();
    }

    @Before
    public void setup() throws Throwable
    {
        temporaryFolder.create();
        descriptor = Descriptor.fromFile(new File(temporaryFolder.newFolder().getAbsolutePath() + "/nb-1-big-Data.db"));
    }

    @After
    public void teardown() throws Throwable
    {
        temporaryFolder.delete();
    }

    @Test
    public void versionAAPerSSTableComponentIsParsedCorrectly() throws Throwable
    {
        createFileOnDisk("-SAI+aa+GroupComplete.db");

        IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor, Murmur3Partitioner.instance, SAITester.EMPTY_COMPARATOR);

        assertEquals(Version.AA, indexDescriptor.version);
        assertTrue(indexDescriptor.hasComponent(IndexComponent.GROUP_COMPLETION_MARKER));
    }

    @Test
    public void versionAAPerIndexComponentIsParsedCorrectly() throws Throwable
    {
        createFileOnDisk("-SAI+aa+test_index+ColumnComplete.db");

        IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor, Murmur3Partitioner.instance, SAITester.EMPTY_COMPARATOR);
        IndexIdentifier indexIdentifier = SAITester.createIndexIdentifier("test", "test", "test_index");

        assertEquals(Version.AA, indexDescriptor.version);
        assertTrue(indexDescriptor.hasComponent(IndexComponent.COLUMN_COMPLETION_MARKER, indexIdentifier));
    }

    @Test
    public void analyzedIndexComponentIsResolvedUnderItsOwnVersion() throws Throwable
    {
        // the sstable's own components stay on version aa while the analyzed index's per-column
        // components live side by side under version ab
        createFileOnDisk("-SAI+aa+GroupComplete.db");
        createFileOnDisk("-SAI+aa+plain_index+ColumnComplete.db");
        createFileOnDisk("-SAI+ab+analyzed_index+ColumnComplete.db");
        createFileOnDisk("-SAI+ab+analyzed_index+Positions.db");

        IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor, Murmur3Partitioner.instance, SAITester.EMPTY_COMPARATOR);
        IndexIdentifier plainIdentifier = SAITester.createIndexIdentifier("test", "test", "plain_index");
        IndexIdentifier analyzedIdentifier = new IndexIdentifier("test", "test", "analyzed_index", Version.AB);

        assertEquals(Version.AA, indexDescriptor.version);
        assertEquals(Version.AA, indexDescriptor.perIndexVersion(plainIdentifier));
        assertEquals(Version.AB, indexDescriptor.perIndexVersion(analyzedIdentifier));
        assertTrue(indexDescriptor.hasComponent(IndexComponent.COLUMN_COMPLETION_MARKER, plainIdentifier));
        assertTrue(indexDescriptor.hasComponent(IndexComponent.COLUMN_COMPLETION_MARKER, analyzedIdentifier));
        assertTrue(indexDescriptor.hasComponent(IndexComponent.POSITIONS, analyzedIdentifier));
        assertFalse(indexDescriptor.hasComponent(IndexComponent.POSITIONS, plainIdentifier));
    }

    @Test
    public void analyzedVersionIsNotAStockVersion()
    {
        // Version.ALL holds only AA, so Version.parse rejects ab
        assertEquals(ImmutableSet.of(Version.AA), Version.ALL);
        assertThatThrownBy(() -> Version.parse("ab"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The version string ab does not represent a valid SAI version. It should be one of aa");
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

    @Test
    public void analyzedHeaderIsAcceptedOnlyUnderTheAnalyzedVersion() throws Throwable
    {
        File analyzed = new File(temporaryFolder.newFile());
        try (IndexOutputWriter writer = IndexFileUtils.instance.openOutput(analyzed))
        {
            SAICodecUtils.writeHeader(writer, Version.AB);
            SAICodecUtils.writeFooter(writer);
        }

        try (IndexInput input = IndexFileUtils.instance.openBlockingInput(analyzed))
        {
            SAICodecUtils.validate(input, Version.AB);
        }

        try (IndexInput input = IndexFileUtils.instance.openBlockingInput(analyzed))
        {
            assertThatThrownBy(() -> SAICodecUtils.validate(input))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("The version string ab does not represent a valid SAI version");
        }

        try (IndexInput input = IndexFileUtils.instance.openBlockingInput(analyzed))
        {
            assertThatThrownBy(() -> SAICodecUtils.validate(input, (Version) null))
            .isInstanceOf(IllegalArgumentException.class);
        }

        try (IndexInput input = IndexFileUtils.instance.openBlockingInput(analyzed))
        {
            assertThatThrownBy(() -> SAICodecUtils.validate(input, Version.AA))
            .isInstanceOf(IllegalArgumentException.class);
        }

        // An analyzed index still reads its components written under the stock version
        File stock = new File(temporaryFolder.newFile());
        try (IndexOutputWriter writer = IndexFileUtils.instance.openOutput(stock))
        {
            SAICodecUtils.writeHeader(writer);
            SAICodecUtils.writeFooter(writer);
        }

        try (IndexInput input = IndexFileUtils.instance.openBlockingInput(stock))
        {
            SAICodecUtils.validate(input, Version.AB);
        }
    }

    private void createFileOnDisk(String filename) throws Throwable
    {
        Path path;
        try
        {
            path = Paths.get(URI.create(descriptor.baseFile() + filename));
        }
        catch (IllegalArgumentException ex)
        {
            path = Paths.get(descriptor.baseFile() + filename);
        }

        Files.touch(new File(path).toJavaIOFile());
    }
}
