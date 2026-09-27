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

package org.apache.cassandra.index.sai.functional;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.EmptyIndex;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class GroupComponentsTest extends SAITester
{
    @Test
    public void testInvalidateWithoutObsolete()
    {
        createTable("CREATE TABLE %s (pk int primary key, value text)");
        createIndex("CREATE INDEX ON %s(value) USING 'sai'");
        execute("INSERT INTO %s (pk) VALUES (1)");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        assertNotNull(group);

        StorageAttachedIndex index = (StorageAttachedIndex) group.getIndexes().iterator().next();
        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());

        Set<Component> components = StorageAttachedIndexGroup.getLiveComponents(sstable, getIndexesFromGroup(group));
        assertEquals(Version.LATEST.onDiskFormat().perSSTableIndexComponents(false).size() + 1, components.size());

        // index files are released but not removed
        cfs.invalidate(true, false);
        Assert.assertTrue(index.view().getIndexes().stream().allMatch(i -> i instanceof EmptyIndex));
        for (Component component : components)
            Assert.assertTrue(sstable.descriptor.fileFor(component).exists());
    }

    @Test
    public void getLiveComponentsForEmptyIndex()
    {
        createTable("CREATE TABLE %s (pk int primary key, value text)");
        createIndex("CREATE INDEX ON %s(value) USING 'sai'");
        execute("INSERT INTO %s (pk) VALUES (1)");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        assertNotNull(group);

        Set<SSTableReader> sstables = cfs.getLiveSSTables();

        assertEquals(1, sstables.size());

        Set<Component> components = StorageAttachedIndexGroup.getLiveComponents(sstables.iterator().next(), getIndexesFromGroup(group));

        assertEquals(Version.LATEST.onDiskFormat().perSSTableIndexComponents(false).size() + 1, components.size());
    }

    @Test
    public void getLiveComponentsForPopulatedIndex()
    {
        createTable("CREATE TABLE %s (pk int primary key, value text)");

        createIndex("CREATE INDEX ON %s(value) USING 'sai'");
        IndexTermType indexTermType = createIndexTermType(UTF8Type.instance);

        execute("INSERT INTO %s (pk, value) VALUES (1, '1')");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        assertNotNull(group);

        Set<SSTableReader> sstables = cfs.getLiveSSTables();

        assertEquals(1, sstables.size());

        Set<Component> components = StorageAttachedIndexGroup.getLiveComponents(sstables.iterator().next(), getIndexesFromGroup(group));

        assertEquals(Version.LATEST.onDiskFormat().perSSTableIndexComponents(false).size() +
                     Version.LATEST.onDiskFormat().perColumnIndexComponents(indexTermType).size(),
                     components.size());
    }

    @Test
    public void getComponentsIncludesPositionsForAnalyzedIndexOnly()
    {
        createTable("CREATE TABLE %s (pk int primary key, v1 text, v2 text)");
        String plainName = createIndex("CREATE INDEX ON %s(v1) USING 'sai'");
        String analyzedName = createIndex("CREATE INDEX ON %s(v2) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        SecondaryIndexManager indexManager = getCurrentColumnFamilyStore().indexManager;
        StorageAttachedIndex plain = (StorageAttachedIndex) indexManager.getIndexByName(plainName);
        StorageAttachedIndex analyzed = (StorageAttachedIndex) indexManager.getIndexByName(analyzedName);

        // the plain index advertises the stock version aa component set, the analyzed index its
        // own version ab set with positions
        Set<String> plainComponents = plain.getComponents().stream().map(c -> c.name).collect(Collectors.toSet());
        Set<String> analyzedComponents = analyzed.getComponents().stream().map(c -> c.name).collect(Collectors.toSet());

        assertEquals(Version.AA.onDiskFormat().perColumnIndexComponents(plain.termType()).size(), plainComponents.size());
        assertEquals(Version.AB.onDiskFormat().perColumnIndexComponents(analyzed.termType()).size(), analyzedComponents.size());

        Assert.assertTrue(plainComponents.stream().allMatch(name -> name.contains("SAI+aa+")));
        Assert.assertTrue(plainComponents.stream().noneMatch(name -> name.contains("Positions")));
        Assert.assertTrue(analyzedComponents.stream().allMatch(name -> name.contains("SAI+ab+")));
        Assert.assertTrue(analyzedComponents.stream().anyMatch(name -> name.contains("Positions")));

        execute("INSERT INTO %s (pk, v1, v2) VALUES (1, 'apple', 'quick fox')");
        flush();

        SSTableReader sstable = Iterables.getOnlyElement(getCurrentColumnFamilyStore().getLiveSSTables());
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(getCurrentColumnFamilyStore());
        assertNotNull(group);
        Set<Component> live = StorageAttachedIndexGroup.getLiveComponents(sstable, getIndexesFromGroup(group));
        Assert.assertTrue(live.stream().anyMatch(component -> component.name.contains("SAI+ab+") && component.name.contains("Positions")));
    }

    private Collection<StorageAttachedIndex> getIndexesFromGroup(StorageAttachedIndexGroup group)
    {
        return group.getIndexes().stream().map(index -> (StorageAttachedIndex)index).collect(Collectors.toList());
    }
}
