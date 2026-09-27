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

package org.apache.cassandra.db.guardrails;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests the guardrails around the cumulative analyzed size and the analyzed token count of a value
 * written to an SAI index. The guardrails are consulted by
 * {@link org.apache.cassandra.index.sai.analyzer.AnalyzedTermLimits}, which has no CQL write path
 * wiring yet, so these tests exercise the guardrails directly.
 *
 * @see Guardrails#saiAnalyzedSize
 * @see Guardrails#saiAnalyzedTokens
 */
public class GuardrailSaiAnalyzedTermsTest extends GuardrailTester
{
    private static final long SIZE_WARN_THRESHOLD = 1024; // bytes
    private static final long SIZE_FAIL_THRESHOLD = SIZE_WARN_THRESHOLD * 4; // bytes
    private static final long TOKENS_WARN_THRESHOLD = 100;
    private static final long TOKENS_FAIL_THRESHOLD = TOKENS_WARN_THRESHOLD * 4;

    private String previousSizeWarn;
    private String previousSizeFail;
    private long previousTokensWarn;
    private long previousTokensFail;

    @Before
    public void setThresholds()
    {
        previousSizeWarn = guardrails().getSaiAnalyzedSizeWarnThreshold();
        previousSizeFail = guardrails().getSaiAnalyzedSizeFailThreshold();
        previousTokensWarn = guardrails().getSaiAnalyzedTokensWarnThreshold();
        previousTokensFail = guardrails().getSaiAnalyzedTokensFailThreshold();
        guardrails().setSaiAnalyzedSizeThreshold(SIZE_WARN_THRESHOLD + "B", SIZE_FAIL_THRESHOLD + "B");
        guardrails().setSaiAnalyzedTokensThreshold(TOKENS_WARN_THRESHOLD, TOKENS_FAIL_THRESHOLD);
    }

    @After
    public void restoreThresholds()
    {
        guardrails().setSaiAnalyzedSizeThreshold(previousSizeWarn, previousSizeFail);
        guardrails().setSaiAnalyzedTokensThreshold(previousTokensWarn, previousTokensFail);
    }

    @Test
    public void testAnalyzedSizeThreshold() throws Throwable
    {
        assertValid(() -> Guardrails.saiAnalyzedSize.guard(SIZE_WARN_THRESHOLD, "v", false, userClientState));

        assertWarns(() -> Guardrails.saiAnalyzedSize.guard(SIZE_WARN_THRESHOLD + 1, "v", false, userClientState),
                    String.format("Analyzed size of value in column 'v' is %d, this exceeds the warning threshold of %d.",
                                  SIZE_WARN_THRESHOLD + 1, SIZE_WARN_THRESHOLD));

        assertFails(() -> Guardrails.saiAnalyzedSize.guard(SIZE_FAIL_THRESHOLD + 1, "v", false, userClientState),
                    String.format("Analyzed size of value in column 'v' is %d, this exceeds the failure threshold of %d.",
                                  SIZE_FAIL_THRESHOLD + 1, SIZE_FAIL_THRESHOLD));
    }

    @Test
    public void testAnalyzedTokensThreshold() throws Throwable
    {
        assertValid(() -> Guardrails.saiAnalyzedTokens.guard(TOKENS_WARN_THRESHOLD, "v", false, userClientState));

        assertWarns(() -> Guardrails.saiAnalyzedTokens.guard(TOKENS_WARN_THRESHOLD + 1, "v", false, userClientState),
                    String.format("Analyzed token count of value in column 'v' is %d, this exceeds the warning threshold of %d.",
                                  TOKENS_WARN_THRESHOLD + 1, TOKENS_WARN_THRESHOLD));

        assertFails(() -> Guardrails.saiAnalyzedTokens.guard(TOKENS_FAIL_THRESHOLD + 1, "v", false, userClientState),
                    String.format("Analyzed token count of value in column 'v' is %d, this exceeds the failure threshold of %d.",
                                  TOKENS_FAIL_THRESHOLD + 1, TOKENS_FAIL_THRESHOLD));
    }

    @Test
    public void testAnalyzedSizeDisabled() throws Throwable
    {
        guardrails().setSaiAnalyzedSizeThreshold(null, null);

        assertFalse(Guardrails.saiAnalyzedSize.enabled(userClientState));
        assertValid(() -> Guardrails.saiAnalyzedSize.guard(SIZE_FAIL_THRESHOLD * 100, "v", false, userClientState));
    }

    @Test
    public void testAnalyzedTokensDisabled() throws Throwable
    {
        guardrails().setSaiAnalyzedTokensThreshold(-1, -1);

        assertFalse(Guardrails.saiAnalyzedTokens.enabled(userClientState));
        assertValid(() -> Guardrails.saiAnalyzedTokens.guard(TOKENS_FAIL_THRESHOLD * 100, "v", false, userClientState));
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        assertValid(() -> Guardrails.saiAnalyzedSize.guard(SIZE_FAIL_THRESHOLD + 1, "v", false, superClientState));
        assertValid(() -> Guardrails.saiAnalyzedSize.guard(SIZE_FAIL_THRESHOLD + 1, "v", false, systemClientState));
        assertValid(() -> Guardrails.saiAnalyzedTokens.guard(TOKENS_FAIL_THRESHOLD + 1, "v", false, superClientState));
        assertValid(() -> Guardrails.saiAnalyzedTokens.guard(TOKENS_FAIL_THRESHOLD + 1, "v", false, systemClientState));
    }

    @Test
    public void testLiveSetAndGet()
    {
        guardrails().setSaiAnalyzedSizeThreshold("2MiB", "4MiB");
        assertEquals("2MiB", guardrails().getSaiAnalyzedSizeWarnThreshold());
        assertEquals("4MiB", guardrails().getSaiAnalyzedSizeFailThreshold());
        assertTrue(Guardrails.saiAnalyzedSize.enabled(userClientState));

        guardrails().setSaiAnalyzedTokensThreshold(5, 10);
        assertEquals(5, guardrails().getSaiAnalyzedTokensWarnThreshold());
        assertEquals(10, guardrails().getSaiAnalyzedTokensFailThreshold());
        assertTrue(Guardrails.saiAnalyzedTokens.enabled(userClientState));
    }

    @Test
    public void testConfigValidation()
    {
        assertConfigFails(g -> g.setSaiAnalyzedSizeThreshold("2MiB", "1MiB"), "should be lower than the fail threshold");
        assertConfigFails(g -> g.setSaiAnalyzedSizeThreshold("0B", "1MiB"), "0 is not allowed");

        assertConfigFails(g -> g.setSaiAnalyzedTokensThreshold(10, 5), "should be lower than the fail threshold");
        assertConfigFails(g -> g.setSaiAnalyzedTokensThreshold(0, 10), "0 is not allowed");
        assertConfigFails(g -> g.setSaiAnalyzedTokensThreshold(-2, 10), "negative values are not allowed");
    }
}
