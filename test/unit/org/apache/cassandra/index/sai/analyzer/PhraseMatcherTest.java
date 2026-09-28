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

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PhraseMatcherTest
{
    private static final LuceneTextAnalyzer STANDARD = analyzer("standard");
    private static final LuceneTextAnalyzer ENGLISH = analyzer("english");

    @Test
    public void adjacentTokensMatch()
    {
        assertTrue(matches(STANDARD, "quick fox", "the quick fox jumps"));
        assertTrue(matches(STANDARD, "quick fox jumps", "the quick fox jumps"));
        assertTrue(matches(STANDARD, "the quick", "the quick fox jumps"));
    }

    @Test
    public void orderAndDistanceAreStrict()
    {
        assertFalse(matches(STANDARD, "fox quick", "the quick fox jumps"));
        assertFalse(matches(STANDARD, "quick jumps", "the quick fox jumps"));
        assertFalse(matches(STANDARD, "quick fox", "quick brown fox"));
    }

    @Test
    public void singleTokenMatchesAnyOccurrence()
    {
        assertTrue(matches(STANDARD, "fox", "the quick fox jumps"));
        assertFalse(matches(STANDARD, "wolf", "the quick fox jumps"));
    }

    @Test
    public void stopwordGapIsPreserved()
    {
        // The english analyzer indexes 'quick the fox' as quick@0, fox@2, so the two token
        // query quick@0, fox@1 must not match, while the query keeping the gap must
        assertFalse(matches(ENGLISH, "quick fox", "quick the fox"));
        assertTrue(matches(ENGLISH, "quick the fox", "quick the fox"));
    }

    @Test
    public void repeatedQueryTokens()
    {
        assertTrue(matches(STANDARD, "buffalo buffalo", "buffalo buffalo river"));
        assertFalse(matches(STANDARD, "buffalo buffalo", "buffalo river buffalo"));
    }

    @Test
    public void repeatedStoredOccurrencesFindAWitness()
    {
        assertTrue(matches(STANDARD, "fox jumps", "fox sleeps and fox jumps"));
    }

    @Test
    public void emptyQueryMatchesNothing()
    {
        assertFalse(PhraseMatcher.matches(Collections.emptyList(), STANDARD.analyze("anything")));
        assertFalse(matches(ENGLISH, "the", "the quick fox"));
    }

    @Test
    public void emptyStoredValueMatchesNothing()
    {
        assertFalse(matches(ENGLISH, "quick fox", "the of and"));
    }

    @Test
    public void positionsCoreRejectsNegativeWitness()
    {
        // Query gap larger than any stored gap must not underflow into a match
        int[] queryPositions = { 0, 5 };
        int[][] stored = { { 3 }, { 4 } };
        assertFalse(PhraseMatcher.matches(queryPositions, stored));
    }

    private static boolean matches(LuceneTextAnalyzer analyzer, String query, String stored)
    {
        List<AnalyzedToken> queryTokens = analyzer.analyze(query);
        List<AnalyzedToken> storedTokens = analyzer.analyze(stored);
        return PhraseMatcher.matches(queryTokens, storedTokens);
    }

    private static LuceneTextAnalyzer analyzer(String config)
    {
        return new LuceneTextAnalyzer(AnalyzerConfig.parse(AnalyzerConfig.INDEX_ANALYZER_OPTION, config));
    }
}
