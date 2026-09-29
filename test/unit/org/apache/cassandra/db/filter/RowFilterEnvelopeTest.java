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

package org.apache.cassandra.db.filter;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RowFilterEnvelopeTest
{
    /**
     * Fork operator codes start at {@link Operator#FORK_OPERATOR_BASE}. The block is:
     * 200 and 201 for MATCH and PHRASE, 202 to 205 reserved,
     * 206 and 207 for MATCH KEY and PHRASE KEY, 208 reserved as the next free code.
     */
    private static final Set<Integer> RESERVED_FORK_CODES = ImmutableSet.of(202, 203, 204, 205, 208);

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static Map<Operator, Integer> upstreamCodes()
    {
        Map<Operator, Integer> codes = new EnumMap<>(Operator.class);
        codes.put(Operator.EQ, 0);
        codes.put(Operator.GTE, 1);
        codes.put(Operator.GT, 2);
        codes.put(Operator.LTE, 3);
        codes.put(Operator.LT, 4);
        codes.put(Operator.CONTAINS, 5);
        codes.put(Operator.CONTAINS_KEY, 6);
        codes.put(Operator.IN, 7);
        codes.put(Operator.NEQ, 8);
        codes.put(Operator.IS_NOT, 9);
        codes.put(Operator.LIKE_PREFIX, 10);
        codes.put(Operator.LIKE_SUFFIX, 11);
        codes.put(Operator.LIKE_CONTAINS, 12);
        codes.put(Operator.LIKE_MATCHES, 13);
        codes.put(Operator.LIKE, 14);
        codes.put(Operator.ANN, 15);
        return codes;
    }

    @Test
    public void testUpstreamOperatorCodesAreUnchanged()
    {
        for (Map.Entry<Operator, Integer> entry : upstreamCodes().entrySet())
        {
            Operator operator = entry.getKey();
            assertEquals(operator.name(), entry.getValue().intValue(), operator.getValue());
            assertFalse(operator.name(), operator.isForkOperator());
        }
    }

    @Test
    public void testForkOperatorCodes()
    {
        Map<Operator, Integer> upstream = upstreamCodes();
        Set<Integer> seen = new HashSet<>();
        for (Operator operator : Operator.values())
        {
            assertTrue(operator.name() + " reuses code " + operator.getValue(), seen.add(operator.getValue()));
            if (upstream.containsKey(operator))
                continue;

            assertTrue(operator.name() + " has code " + operator.getValue(), operator.getValue() >= Operator.FORK_OPERATOR_BASE);
            assertTrue(operator.name(), operator.isForkOperator());
        }

        // None of these operators may use a reserved code.
        Operator[] forkOperators = { Operator.ANALYZER_MATCHES, Operator.PHRASE, Operator.ANALYZER_MATCHES_KEY, Operator.PHRASE_KEY };
        for (Operator operator : forkOperators)
        {
            assertTrue(operator.name(), operator.isForkOperator());
            assertFalse(operator.name() + " uses reserved code " + operator.getValue(), RESERVED_FORK_CODES.contains(operator.getValue()));
        }
    }

    @Test
    public void testOperatorCodeRoundTrip() throws Exception
    {
        for (Operator operator : Operator.values())
        {
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                operator.writeTo(out);
                assertEquals(operator.serializedSize(), out.getLength());

                try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
                {
                    assertEquals(operator, Operator.readFrom(in));
                }
            }
        }
    }
}
