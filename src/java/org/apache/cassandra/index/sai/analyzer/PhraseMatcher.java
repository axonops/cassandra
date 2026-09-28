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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.carrotsearch.hppc.IntArrayList;

/**
 * Strict, gap-preserving phrase matching over analyzer positions, shared by every place a phrase is
 * checked: the memtable index, the on-disk segment intersection, and the re-analysis post-filters on
 * replica and coordinator.
 * <p>
 * A phrase matches when some witness position {@code w} exists such that for every query token
 * occurrence {@code i} with query position {@code p(i)}, the position {@code w + p(i) - p(0)} is
 * among that token's stored positions. Positions are zero based and gaps left by filters that remove
 * tokens are preserved on both sides, the Lucene {@code PhraseQuery} slop 0 semantics. A query that
 * analyzes to no tokens matches nothing.
 */
public final class PhraseMatcher
{
    private PhraseMatcher()
    {
    }

    /**
     * Matches a query token list against the analyzed tokens of one stored value.
     *
     * @param queryTokens the phrase analyzed by the query analyzer, in emission order
     * @param storedTokens the stored value analyzed by the index analyzer, in emission order
     * @return true when the stored value contains the phrase with exactly matching position gaps
     */
    public static boolean matches(List<AnalyzedToken> queryTokens, List<AnalyzedToken> storedTokens)
    {
        if (queryTokens.isEmpty() || storedTokens.isEmpty())
            return false;

        Map<ByteBuffer, IntArrayList> storedPositions = new HashMap<>();
        for (AnalyzedToken token : storedTokens)
            storedPositions.computeIfAbsent(token.bytes(), bytes -> new IntArrayList()).add(token.position());

        int[] queryPositions = new int[queryTokens.size()];
        int[][] positionsPerOccurrence = new int[queryTokens.size()][];
        Map<ByteBuffer, int[]> arrayPerToken = new HashMap<>();
        for (int i = 0; i < queryTokens.size(); i++)
        {
            AnalyzedToken token = queryTokens.get(i);
            IntArrayList positions = storedPositions.get(token.bytes());
            if (positions == null)
                return false;

            queryPositions[i] = token.position();
            positionsPerOccurrence[i] = arrayPerToken.computeIfAbsent(token.bytes(), bytes -> positions.toArray());
        }
        return matches(queryPositions, positionsPerOccurrence);
    }

    /**
     * Core positional check. Emission order of the analyzer keeps every positions array ascending,
     * which the binary searches below rely on.
     *
     * @param queryPositions the analyzer position of each query token occurrence, in emission order
     * @param positionsPerOccurrence for each query token occurrence, the ascending stored positions
     * of that occurrence's token, arrays repeated for repeated tokens
     * @return true when a witness position satisfies every occurrence
     */
    public static boolean matches(int[] queryPositions, int[][] positionsPerOccurrence)
    {
        assert queryPositions.length == positionsPerOccurrence.length;

        if (queryPositions.length == 0)
            return false;

        // Drive candidates from the rarest token so the work is bounded by its frequency
        int driver = 0;
        for (int i = 1; i < positionsPerOccurrence.length; i++)
        {
            if (positionsPerOccurrence[i].length < positionsPerOccurrence[driver].length)
                driver = i;
        }

        for (int position : positionsPerOccurrence[driver])
        {
            int witness = position - (queryPositions[driver] - queryPositions[0]);
            if (witnessedBy(witness, queryPositions, positionsPerOccurrence))
                return true;
        }
        return false;
    }

    private static boolean witnessedBy(int witness, int[] queryPositions, int[][] positionsPerOccurrence)
    {
        for (int i = 0; i < queryPositions.length; i++)
        {
            int required = witness + (queryPositions[i] - queryPositions[0]);
            if (required < 0 || Arrays.binarySearch(positionsPerOccurrence[i], required) < 0)
                return false;
        }
        return true;
    }
}
