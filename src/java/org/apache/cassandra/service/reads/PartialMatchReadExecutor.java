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
package org.apache.cassandra.service.reads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.reads.repair.NoopReadRepair;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Dispatcher;

/**
 * Reads one partition for a filtering query whose replicas may return rows that match only part of the filter
 * (see RowFilter#acceptsPartialMatches). Every request, speculative ones included, asks for data, never a
 * digest. Responses that agree are merged and filtered again on the coordinator too, because each may hold
 * rows that fail the filter. When they disagree the stock read repair round runs, which also resolves through
 * replica filtering protection.
 */
class PartialMatchReadExecutor extends AbstractReadExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(PartialMatchReadExecutor.class);

    // The speculation getReadExecutor would have chosen for this read
    private enum Speculation { NEVER, NEVER_RECORD_INSUFFICIENT, ON_LATENCY, ALWAYS }

    private final Speculation speculation;

    private PartialMatchReadExecutor(ColumnFamilyStore cfs, ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan,
                                     Dispatcher.RequestTime requestTime, Speculation speculation)
    {
        super(cfs, command, replicaPlan, replicaPlan.contacts().size(), requestTime);
        this.speculation = speculation;
    }

    static boolean appliesTo(SinglePartitionReadCommand command)
    {
        return command.rowFilter().acceptsPartialMatches();
    }

    static AbstractReadExecutor create(ColumnFamilyStore cfs, SinglePartitionReadCommand command, ReplicaPlan.ForTokenRead replicaPlan,
                                       SpeculativeRetryPolicy retry, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
    {
        // The same choice as getReadExecutor
        Speculation speculation;
        if (retry.equals(NeverSpeculativeRetryPolicy.INSTANCE) || consistencyLevel == ConsistencyLevel.EACH_QUORUM)
            speculation = Speculation.NEVER;
        else if (replicaPlan.contacts().size() == replicaPlan.readCandidates().size())
            speculation = consistencyLevel != ConsistencyLevel.ALL ? Speculation.NEVER_RECORD_INSUFFICIENT : Speculation.NEVER;
        else if (retry.equals(AlwaysSpeculativeRetryPolicy.INSTANCE))
            speculation = Speculation.ALWAYS;
        else
            speculation = Speculation.ON_LATENCY;

        return new PartialMatchReadExecutor(cfs, command, replicaPlan, requestTime, speculation);
    }

    @Override
    public void executeAsync()
    {
        super.executeAsync();
        if (speculation == Speculation.ALWAYS)
            cfs.metric.speculativeRetries.inc();
    }

    @Override
    public void maybeTryAdditionalReplicas()
    {
        switch (speculation)
        {
            case NEVER:
                shouldSpeculateAndMaybeWait();
                return;
            case NEVER_RECORD_INSUFFICIENT:
                if (shouldSpeculateAndMaybeWait())
                    cfs.metric.speculativeInsufficientReplicas.inc();
                return;
            case ALWAYS:
                return;
            case ON_LATENCY:
                if (shouldSpeculateAndMaybeWait())
                    speculate();
        }
    }

    private void speculate()
    {
        cfs.metric.speculativeRetries.inc();

        // As stock, a full replica unless data is already present. The request is always for data.
        Replica extraReplica = handler.resolver.isDataPresent()
                               ? replicaPlan().firstUncontactedCandidate(replica -> true)
                               : replicaPlan().firstUncontactedCandidate(Replica::isFull);
        if (extraReplica == null)
        {
            cfs.metric.speculativeInsufficientReplicas.inc();
            return;
        }

        ReadCommand retryCommand = extraReplica.isTransient() ? command.copyAsTransientQuery(extraReplica) : command;
        sharedReplicaPlan().addToContacts(extraReplica);

        if (traceState != null)
            traceState.trace("speculating read retry on {}", extraReplica);
        logger.trace("speculating read retry on {}", extraReplica);

        MessagingService.instance().sendWithCallback(retryCommand.createMessage(false, requestTime), extraReplica.endpoint(), handler);
    }

    @Override
    void onReadTimeout()
    {
        // Counts failed retries as SpeculatingReadExecutor and AlwaysSpeculatingReadExecutor do with assertions off
        if (speculation == Speculation.ALWAYS || speculation == Speculation.ON_LATENCY)
            cfs.metric.speculativeFailedRetries.inc();
    }

    @Override
    public void awaitResponses(boolean logBlockingReadRepairAttempt) throws ReadTimeoutException
    {
        try
        {
            handler.awaitResults();
        }
        catch (ReadTimeoutException e)
        {
            try
            {
                onReadTimeout();
            }
            finally
            {
                throw e;
            }
        }

        assert digestResolver.isDataPresent() : "awaitResults returned with no data present.";

        if (digestResolver.responsesMatch())
        {
            // As DigestResolver.getData on a match: merged and filtered, never repaired
            @SuppressWarnings("unchecked")
            DataResolver<EndpointsForToken, ReplicaPlan.ForTokenRead> resolver =
                new DataResolver<>(command, sharedReplicaPlan(), (NoopReadRepair<EndpointsForToken, ReplicaPlan.ForTokenRead>) NoopReadRepair.instance, requestTime);
            for (Message<ReadResponse> response : digestResolver.getMessages().snapshot())
                resolver.preprocess(response);
            setResult(resolver.resolve());
            return;
        }

        Tracing.trace("Digest mismatch: Mismatch for key {}", getKey());
        readRepair.startRepair(digestResolver, this::setResult);
        if (logBlockingReadRepairAttempt)
        {
            logger.info("Blocking Read Repair triggered for query [{}] at CL.{} with endpoints {}",
                        command.toCQLString(),
                        replicaPlan().consistencyLevel(),
                        replicaPlan().contacts());
        }
    }
}
