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

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ClusterVersionGateTest
{
    @Test
    public void noPeersMeansSupported() throws UnknownHostException
    {
        assertNull(ClusterVersionGate.firstLaggingPeer(Collections.emptyList(), self(), peer -> true, peer -> MessagingService.VERSION_AXON_50));
    }

    @Test
    public void selfIsNotAPeer() throws UnknownHostException
    {
        InetAddressAndPort self = self();
        assertNull(ClusterVersionGate.firstLaggingPeer(List.of(self), self, peer -> false, peer -> MessagingService.VERSION_40));
    }

    @Test
    public void allPeersOnForkVersionAreSupported() throws UnknownHostException
    {
        List<InetAddressAndPort> peers = Arrays.asList(peer(2), peer(3));
        assertNull(ClusterVersionGate.firstLaggingPeer(peers, self(), peer -> true, peer -> MessagingService.VERSION_AXON_50));
    }

    @Test
    public void vanillaPeerIsLagging() throws UnknownHostException
    {
        InetAddressAndPort vanilla = peer(3);
        Map<InetAddressAndPort, Integer> versions = Map.of(peer(2), MessagingService.VERSION_AXON_50,
                                                           vanilla, MessagingService.VERSION_50);
        List<InetAddressAndPort> peers = Arrays.asList(peer(2), vanilla);
        assertEquals(vanilla, ClusterVersionGate.firstLaggingPeer(peers, self(), versions::containsKey, versions::get));
    }

    @Test
    public void unknownVersionIsLagging() throws UnknownHostException
    {
        InetAddressAndPort unknown = peer(2);
        List<InetAddressAndPort> peers = Collections.singletonList(unknown);
        assertEquals(unknown, ClusterVersionGate.firstLaggingPeer(peers, self(), peer -> false, peer -> {
            throw new AssertionError("The version of an unknown peer must not be read");
        }));
    }

    private static InetAddressAndPort self() throws UnknownHostException
    {
        return peer(1);
    }

    private static InetAddressAndPort peer(int lastOctet) throws UnknownHostException
    {
        return InetAddressAndPort.getByName("127.0.0." + lastOctet);
    }
}
