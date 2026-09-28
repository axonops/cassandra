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

package org.apache.cassandra.index.sai;

import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Cluster-wide upgrade gate for the SAI full-text query features. The analyzed operators {@code MATCH}
 * and {@code PHRASE}, and {@code =} when rewritten by {@code equals_behaviour_when_analyzed}, are
 * refused until every live peer runs a build that advertises
 * {@link MessagingService#VERSION_AXON_50}. A vanilla replica would evaluate the new operators
 * wrongly or fail the read mid-flight, so the statement layer consults this gate before adding one
 * of them to a row filter.
 * <p>
 * The check is conservative: a live peer whose messaging version is not yet known counts as not
 * upgraded. Peer versions are learned from both the internode handshake and the gossip
 * {@code NET_VERSION} state, so on a healthy cluster they are known soon after startup.
 */
public final class ClusterVersionGate
{
    public static final String NODES_MUST_RUN_THIS_BUILD_MESSAGE =
        "%s is not supported until all nodes in the cluster are running this build (node %s is not)";

    private ClusterVersionGate()
    {
    }

    /**
     * Refuses the given query feature unless every live peer speaks the fork messaging version.
     *
     * @param feature description of the refused feature, used in the error message
     * @throws InvalidRequestException when a live peer does not, or is not known to, run this build
     */
    public static void checkClusterSupports(String feature)
    {
        InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
        Iterable<InetAddressAndPort> peers = Gossiper.instance.getLiveMembers();
        InetAddressAndPort lagging = firstLaggingPeer(peers,
                                                      self,
                                                      MessagingService.instance().versions::knows,
                                                      MessagingService.instance().versions::getRaw);
        if (lagging != null)
            throw new InvalidRequestException(String.format(NODES_MUST_RUN_THIS_BUILD_MESSAGE, feature, lagging));
    }

    /**
     * @return the first peer that is not known to speak {@link MessagingService#VERSION_AXON_50},
     * or null when every peer does
     */
    @VisibleForTesting
    @Nullable
    static InetAddressAndPort firstLaggingPeer(Iterable<InetAddressAndPort> peers,
                                               InetAddressAndPort self,
                                               Predicate<InetAddressAndPort> knowsVersion,
                                               ToIntFunction<InetAddressAndPort> rawVersion)
    {
        for (InetAddressAndPort peer : peers)
        {
            if (peer.equals(self))
                continue;

            if (!knowsVersion.test(peer) || rawVersion.applyAsInt(peer) < MessagingService.VERSION_AXON_50)
                return peer;
        }
        return null;
    }
}
