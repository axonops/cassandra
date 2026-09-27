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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * The single limits rule for analyzed values. One instance per index evaluates a value's complete
 * token list, so the verdict is identical at memtable insert, flush and compaction: a value is
 * either fully indexed or not indexed at all, never a token subset.
 *
 * <p>Three bounds apply. The per token size uses the existing {@code sai_string_term_size}
 * guardrail unchanged. The cumulative analyzed size per value uses the {@code sai_analyzed_size}
 * guardrail and the token count per value uses the {@code sai_analyzed_tokens} guardrail, which
 * ships disabled.
 */
public final class AnalyzedTermLimits
{
    /**
     * The bound on one row's analyzed collection elements. Element position bases advance by at
     * least {@link LuceneTextAnalyzer#POSITION_GAP} per element, so this bound keeps every base
     * within an int.
     */
    public static final int MAX_ELEMENTS_PER_ROW = Integer.MAX_VALUE / LuceneTextAnalyzer.POSITION_GAP;

    public static final String VALUE_DROPPED_MESSAGE = "Value in column '%s' for key '%s' breaches the analyzed " +
                                                       "term limits and the whole value was not indexed. " +
                                                       "(analyzed size: %s, tokens: %s)";

    public static final String ELEMENT_COUNT_MESSAGE = "Row for key '%s' breaches the bound of %s analyzed " +
                                                       "collection elements per row on column '%s'";

    private static final Logger logger = LoggerFactory.getLogger(AnalyzedTermLimits.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    private static final LongAdder analyzerConfigErrors = new LongAdder();

    private final IndexIdentifier indexIdentifier;
    private final IndexTermType indexTermType;
    private final LongAdder droppedValues = new LongAdder();
    private final LongAdder oversizeTokens = new LongAdder();

    public AnalyzedTermLimits(IndexIdentifier indexIdentifier, IndexTermType indexTermType)
    {
        this.indexIdentifier = indexIdentifier;
        this.indexTermType = indexTermType;
    }

    /**
     * Evaluates a value's complete token list against the per token size, cumulative analyzed
     * size and token count bounds. Client mutations that breach a fail threshold throw
     * {@link org.apache.cassandra.db.guardrails.GuardrailViolatedException}, and a warn
     * threshold breach warns the client.
     *
     * @param tokens the value's complete token list
     * @param source lazy description of the value's key for log messages, only read on a breach
     * @param isClientMutation whether the value arrives in a client mutation
     * @param state the client state, may be null
     * @return true when the value may be indexed, false when a non client path must skip the
     * whole value
     */
    public boolean validate(List<AnalyzedToken> tokens,
                            Supplier<String> source,
                            boolean isClientMutation,
                            @Nullable ClientState state)
    {
        long analyzedSize = 0;
        for (AnalyzedToken token : tokens)
            analyzedSize += token.size();

        if (isClientMutation)
        {
            for (AnalyzedToken token : tokens)
                Guardrails.saiStringTermSize.guard(token.size(), indexTermType.columnName(), false, state);

            Guardrails.saiAnalyzedSize.guard(analyzedSize, indexTermType.columnName(), false, state);
            Guardrails.saiAnalyzedTokens.guard(tokens.size(), indexTermType.columnName(), false, state);
            return true;
        }

        boolean dropped = false;
        for (AnalyzedToken token : tokens)
        {
            if (Guardrails.saiStringTermSize.failsOn(token.size(), state))
            {
                countOversizeToken();
                dropped = true;
            }
        }

        dropped |= Guardrails.saiAnalyzedSize.failsOn(analyzedSize, state);
        dropped |= Guardrails.saiAnalyzedTokens.failsOn(tokens.size(), state);

        if (dropped)
        {
            countDroppedValue();
            noSpamLogger.warn(indexIdentifier.logMessage(String.format(VALUE_DROPPED_MESSAGE,
                                                                       indexTermType.columnName(),
                                                                       source.get(),
                                                                       FBUtilities.prettyPrintMemory(analyzedSize),
                                                                       tokens.size())));
            return false;
        }
        return true;
    }

    /**
     * Bounds one row's analyzed collection elements at {@link #MAX_ELEMENTS_PER_ROW} so element
     * position bases can never overflow an int. Client mutations breaching the bound are rejected,
     * non client paths index the row's first {@link #MAX_ELEMENTS_PER_ROW} elements and drop each
     * further value, counted and logged.
     *
     * @param elementOrdinal the zero based ordinal of the value within its row
     * @param source lazy description of the value's key for messages, only read on a breach
     * @param isClientMutation whether the value arrives in a client mutation
     * @return true when the value may be indexed, false when a non client path must skip it
     * @throws InvalidRequestException when a client mutation breaches the bound
     */
    public boolean validateElementOrdinal(int elementOrdinal, Supplier<String> source, boolean isClientMutation)
    {
        if (elementOrdinal < MAX_ELEMENTS_PER_ROW)
            return true;

        if (isClientMutation)
            throw new InvalidRequestException(String.format(ELEMENT_COUNT_MESSAGE,
                                                            source.get(),
                                                            MAX_ELEMENTS_PER_ROW,
                                                            indexTermType.columnName()));

        countDroppedValue();
        noSpamLogger.warn(indexIdentifier.logMessage(String.format(ELEMENT_COUNT_MESSAGE + " and its remaining values were not indexed",
                                                                   source.get(),
                                                                   MAX_ELEMENTS_PER_ROW,
                                                                   indexTermType.columnName())));
        return false;
    }

    public long droppedValueCount()
    {
        return droppedValues.sum();
    }

    public long oversizeTokenCount()
    {
        return oversizeTokens.sum();
    }

    public static long analyzerConfigErrorCount()
    {
        return analyzerConfigErrors.sum();
    }

    /**
     * Metric hook, the IndexMetrics counter named AnalyzedValuesDropped registers here.
     */
    private void countDroppedValue()
    {
        droppedValues.increment();
    }

    /**
     * Metric hook, the IndexMetrics counter named OversizeAnalyzedTokens registers here.
     */
    private void countOversizeToken()
    {
        oversizeTokens.increment();
    }

    /**
     * Metric hook called wherever an analyzer config is rejected, the global counter named
     * AnalyzerConfigErrors registers here.
     */
    static void countAnalyzerConfigError()
    {
        analyzerConfigErrors.increment();
    }
}
