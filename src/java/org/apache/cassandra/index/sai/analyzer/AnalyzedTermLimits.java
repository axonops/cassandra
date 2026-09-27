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
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * The single limits rule for analyzed values. One instance per index evaluates a value's complete
 * token list, so the verdict is identical at memtable insert, flush and compaction: a value is
 * either fully indexed or not indexed at all, never a token subset.
 *
 * <p>Three bounds apply. The per token size uses the existing {@code sai_string_term_size}
 * guardrail unchanged. The cumulative analyzed size and the token count per value are constructor
 * parameters pending guardrail integration. The token count bound ships disabled.
 */
public final class AnalyzedTermLimits
{
    public static final long DEFAULT_ANALYZED_SIZE_WARN_BYTES = 1024 * 1024;
    public static final long DEFAULT_ANALYZED_SIZE_FAIL_BYTES = 8 * 1024 * 1024;

    /** Thresholds at or below zero are disabled, matching guardrail semantics */
    public static final long DISABLED = -1;

    public static final String VALUE_DROPPED_MESSAGE = "Value in column '%s' for key '%s' breaches the analyzed " +
                                                       "term limits and the whole value was not indexed. " +
                                                       "(analyzed size: %s, tokens: %s)";

    private static final Logger logger = LoggerFactory.getLogger(AnalyzedTermLimits.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    private static final LongAdder analyzerConfigErrors = new LongAdder();

    private final IndexIdentifier indexIdentifier;
    private final IndexTermType indexTermType;
    private final long analyzedSizeWarnBytes;
    private final long analyzedSizeFailBytes;
    private final long tokenCountWarn;
    private final long tokenCountFail;
    private final LongAdder droppedValues = new LongAdder();
    private final LongAdder oversizeTokens = new LongAdder();

    public AnalyzedTermLimits(IndexIdentifier indexIdentifier, IndexTermType indexTermType)
    {
        this(indexIdentifier, indexTermType,
             DEFAULT_ANALYZED_SIZE_WARN_BYTES, DEFAULT_ANALYZED_SIZE_FAIL_BYTES,
             DISABLED, DISABLED);
    }

    public AnalyzedTermLimits(IndexIdentifier indexIdentifier,
                              IndexTermType indexTermType,
                              long analyzedSizeWarnBytes,
                              long analyzedSizeFailBytes,
                              long tokenCountWarn,
                              long tokenCountFail)
    {
        this.indexIdentifier = indexIdentifier;
        this.indexTermType = indexTermType;
        this.analyzedSizeWarnBytes = analyzedSizeWarnBytes;
        this.analyzedSizeFailBytes = analyzedSizeFailBytes;
        this.tokenCountWarn = tokenCountWarn;
        this.tokenCountFail = tokenCountFail;
    }

    /**
     * Evaluates a value's complete token list against the per token size, cumulative analyzed
     * size and token count bounds.
     *
     * @param tokens the value's complete token list
     * @param source lazy description of the value's key for log messages, only read on a breach
     * @param isClientMutation whether the value arrives in a client mutation
     * @param state the client state, may be null
     * @return true when the value may be indexed, false when a non client path must skip the
     * whole value
     * @throws InvalidRequestException for client mutations that breach a fail threshold, and
     * warns the client on a warn threshold breach
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

            guardSeamThreshold(analyzedSize, analyzedSizeWarnBytes, analyzedSizeFailBytes, this::analyzedSizeMessage, state);
            guardSeamThreshold(tokens.size(), tokenCountWarn, tokenCountFail, this::tokenCountMessage, state);
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

        dropped |= breaches(analyzedSize, analyzedSizeFailBytes);
        dropped |= breaches(tokens.size(), tokenCountFail);

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

    private static boolean breaches(long value, long threshold)
    {
        return threshold > 0 && value > threshold;
    }

    /**
     * Mirrors {@link org.apache.cassandra.db.guardrails.Threshold#guard} for the two thresholds
     * that are not guardrails yet: a fail breach throws only for a query with a client state, a
     * warn breach warns the client.
     */
    private void guardSeamThreshold(long value,
                                    long warnThreshold,
                                    long failThreshold,
                                    SeamMessageProvider messageProvider,
                                    @Nullable ClientState state)
    {
        if (breaches(value, failThreshold))
        {
            String message = messageProvider.create(false, value, failThreshold);
            noSpamLogger.error(indexIdentifier.logMessage(message));
            if (state != null)
                throw new InvalidRequestException(message);
            return;
        }
        if (breaches(value, warnThreshold))
            ClientWarn.instance.warn(messageProvider.create(true, value, warnThreshold));
    }

    private interface SeamMessageProvider
    {
        String create(boolean isWarning, long value, long threshold);
    }

    private String analyzedSizeMessage(boolean isWarning, long value, long threshold)
    {
        return String.format("Analyzed size of value in column '%s' is %s, this exceeds the %s threshold of %s.",
                             indexTermType.columnName(),
                             FBUtilities.prettyPrintMemory(value),
                             isWarning ? "warning" : "failure",
                             FBUtilities.prettyPrintMemory(threshold));
    }

    private String tokenCountMessage(boolean isWarning, long value, long threshold)
    {
        return String.format("Analyzed token count of value in column '%s' is %s, this exceeds the %s threshold of %s.",
                             indexTermType.columnName(),
                             value,
                             isWarning ? "warning" : "failure",
                             threshold);
    }
}
