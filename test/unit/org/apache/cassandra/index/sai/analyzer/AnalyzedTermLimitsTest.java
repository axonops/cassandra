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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AnalyzedTermLimitsTest
{
    private static ClientState clientState;
    private static IAuthenticator previousAuthenticator;

    @BeforeClass
    public static void initialize()
    {
        DatabaseDescriptor.daemonInitialization();

        // guardrails only apply to ordinary users, which requires an authenticating authenticator
        // and a logged in user that is not super
        previousAuthenticator = DatabaseDescriptor.getAuthenticator();
        DatabaseDescriptor.setAuthenticator(new PasswordAuthenticator());
        clientState = ClientState.forExternalCalls(InetSocketAddress.createUnresolved("127.0.0.1", 9042));
        clientState.login(new AuthenticatedUser("test_user")
        {
            @Override
            public boolean canLogin()
            {
                return true;
            }

            @Override
            public boolean isSuper()
            {
                return false;
            }
        });
    }

    @AfterClass
    public static void restore()
    {
        DatabaseDescriptor.setAuthenticator(previousAuthenticator);
    }

    @Test
    public void clientMutationFailThresholdThrows()
    {
        // the default sai_string_term_size fail threshold is 8KiB
        AnalyzedTermLimits limits = limits();
        List<AnalyzedToken> tokens = tokens(9 * 1024);

        assertThatThrownBy(() -> limits.validate(tokens, () -> "key", true, clientState))
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void clientMutationWarnThresholdWarns()
    {
        // the default sai_string_term_size warn threshold is 1KiB
        AnalyzedTermLimits limits = limits();
        List<AnalyzedToken> tokens = tokens(2 * 1024);

        ClientWarn.instance.captureWarnings();
        try
        {
            assertTrue(limits.validate(tokens, () -> "key", true, clientState));
            List<String> warnings = ClientWarn.instance.getWarnings();
            assertNotNull(warnings);
            assertEquals(1, warnings.size());
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
        }
    }

    @Test
    public void nonClientBreachDropsWholeValueAndCounts()
    {
        AnalyzedTermLimits limits = limits();
        List<AnalyzedToken> tokens = tokens(9 * 1024, 10);

        assertFalse(limits.validate(tokens, () -> "key", false, null));
        assertEquals(1, limits.droppedValueCount());
    }

    @Test
    public void oversizeTokenCountsAndDropsValue()
    {
        AnalyzedTermLimits limits = limits();
        List<AnalyzedToken> tokens = tokens(9 * 1024, 9 * 1024, 10);

        assertFalse(limits.validate(tokens, () -> "key", false, null));
        assertEquals(1, limits.droppedValueCount());
        assertEquals(2, limits.oversizeTokenCount());
    }

    @Test
    public void cumulativeSizeThresholdApplies()
    {
        AnalyzedTermLimits limits = limits();
        List<AnalyzedToken> tokens = tokens(10, 10, 10);

        String previousWarn = Guardrails.instance.getSaiAnalyzedSizeWarnThreshold();
        String previousFail = Guardrails.instance.getSaiAnalyzedSizeFailThreshold();
        Guardrails.instance.setSaiAnalyzedSizeThreshold("10B", "20B");
        try
        {
            assertFalse(limits.validate(tokens, () -> "key", false, null));
            assertEquals(1, limits.droppedValueCount());
            assertEquals(0, limits.oversizeTokenCount());

            assertThatThrownBy(() -> limits.validate(tokens, () -> "key", true, clientState))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageContaining("Analyzed size");
        }
        finally
        {
            Guardrails.instance.setSaiAnalyzedSizeThreshold(previousWarn, previousFail);
        }
    }

    @Test
    public void tokenCountThresholdDisabledByDefault()
    {
        AnalyzedTermLimits limits = limits();
        List<AnalyzedToken> tokens = tokens(new int[1000]);

        assertTrue(limits.validate(tokens, () -> "key", false, null));
        assertTrue(limits.validate(tokens, () -> "key", true, clientState));
        assertEquals(0, limits.droppedValueCount());
    }

    @Test
    public void tokenCountThresholdApplies()
    {
        AnalyzedTermLimits limits = limits();
        List<AnalyzedToken> tokens = tokens(1, 1, 1, 1, 1, 1);

        long previousWarn = Guardrails.instance.getSaiAnalyzedTokensWarnThreshold();
        long previousFail = Guardrails.instance.getSaiAnalyzedTokensFailThreshold();
        Guardrails.instance.setSaiAnalyzedTokensThreshold(3, 5);
        try
        {
            assertFalse(limits.validate(tokens, () -> "key", false, null));
            assertEquals(1, limits.droppedValueCount());

            assertThatThrownBy(() -> limits.validate(tokens, () -> "key", true, clientState))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageContaining("token count");
        }
        finally
        {
            Guardrails.instance.setSaiAnalyzedTokensThreshold(previousWarn, previousFail);
        }
    }

    @Test
    public void sameVerdictRegardlessOfPath()
    {
        AnalyzedTermLimits limits = limits();
        List<AnalyzedToken> tokens = tokens(9 * 1024);

        assertFalse(limits.validate(tokens, () -> "key", false, null));
        assertFalse(limits.validate(tokens, () -> "key", false, null));
        assertEquals(2, limits.droppedValueCount());
    }

    private static AnalyzedTermLimits limits()
    {
        return new AnalyzedTermLimits(identifier(), SAITester.createIndexTermType(UTF8Type.instance));
    }

    private static IndexIdentifier identifier()
    {
        return new IndexIdentifier("ks", "tbl", "idx");
    }

    private static List<AnalyzedToken> tokens(int... sizes)
    {
        List<AnalyzedToken> tokens = new ArrayList<>(sizes.length);
        for (int i = 0; i < sizes.length; i++)
            tokens.add(new AnalyzedToken(ByteBuffer.allocate(Math.max(sizes[i], 1)), i));
        return tokens;
    }
}
