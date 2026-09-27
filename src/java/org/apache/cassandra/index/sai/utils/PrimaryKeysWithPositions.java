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
package org.apache.cassandra.index.sai.utils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.cassandra.utils.ObjectSizes;

/**
 * The trie payload of an analyzed memtable index: a sorted map of {@link PrimaryKey} to that key's
 * token positions for the payload's term. {@link #keys()} presents the map's key set, so every
 * reader of the plain {@link PrimaryKeys} payload works unchanged.
 * <p>
 * Every add for one row carries the row's epoch. A row's newer epoch replaces the positions stored
 * under an older one instead of appending, so per-key positions always describe a single version of
 * the row. Adds within one epoch append, which only happens when two collection elements of one row
 * share a term.
 * <p>
 * All mutation happens under the owning index's synchronized add methods.
 */
@ThreadSafe
public class PrimaryKeysWithPositions extends PrimaryKeys
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new PrimaryKeysWithPositions());
    // from https://github.com/gaul/java-collection-overhead, as for the set in PrimaryKeys
    private static final long MAP_ENTRY_OVERHEAD = 36;
    private static final long POSITION_ENTRY_OVERHEAD = 40;

    private final ConcurrentSkipListMap<PrimaryKey, PositionEntry> keys = new ConcurrentSkipListMap<>();

    @Override
    public long add(PrimaryKey key)
    {
        throw new UnsupportedOperationException("Analyzed indexes must add primary keys with positions");
    }

    /**
     * Adds or replaces the positions of the given key, and returns the on-heap memory the call used
     */
    public long add(PrimaryKey key, int[] positions, long epoch)
    {
        PositionEntry entry = keys.get(key);
        if (entry == null)
        {
            keys.put(key, new PositionEntry(epoch, positions));
            return MAP_ENTRY_OVERHEAD + POSITION_ENTRY_OVERHEAD + arrayBytes(positions);
        }

        if (entry.epoch != epoch)
        {
            // A newer version of the row replaces the older version's positions
            long growth = arrayBytes(positions) - arrayBytes(entry.positions);
            entry.epoch = epoch;
            entry.positions = positions;
            return Math.max(0, growth);
        }

        // Same row, another collection element sharing this term appends its position space
        int[] combined = Arrays.copyOf(entry.positions, entry.positions.length + positions.length);
        System.arraycopy(positions, 0, combined, entry.positions.length, positions.length);
        entry.positions = combined;
        return arrayBytes(positions);
    }

    /**
     * @return the stored positions of the given key, or null when the key is not present
     */
    public PositionEntry entry(PrimaryKey key)
    {
        return keys.get(key);
    }

    public Iterator<Map.Entry<PrimaryKey, PositionEntry>> entries()
    {
        return keys.entrySet().iterator();
    }

    @Override
    public SortedSet<PrimaryKey> keys()
    {
        return keys.navigableKeySet();
    }

    @Override
    public int size()
    {
        return keys.size();
    }

    @Override
    public boolean isEmpty()
    {
        return keys.isEmpty();
    }

    @Override
    public long unsharedHeapSize()
    {
        return EMPTY_SIZE;
    }

    @Override
    public Iterator<PrimaryKey> iterator()
    {
        return keys.navigableKeySet().iterator();
    }

    private static long arrayBytes(int[] positions)
    {
        return 4L * positions.length;
    }

    /**
     * One key's positions for one term, together with the epoch of the row version that wrote them
     */
    public static class PositionEntry
    {
        private long epoch;
        private int[] positions;

        PositionEntry(long epoch, int[] positions)
        {
            this.epoch = epoch;
            this.positions = positions;
        }

        public long epoch()
        {
            return epoch;
        }

        public int[] positions()
        {
            return positions;
        }
    }
}
