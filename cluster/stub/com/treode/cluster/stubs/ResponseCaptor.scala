/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.cluster.stubs

import scala.util.Random

import com.treode.async.Async
import com.treode.pickle.Pickler
import com.treode.cluster._

import Async.async

private class ResponseCaptor (
    val localId: HostId
) (implicit
    random: Random,
    network: StubNetwork
) extends StubPeer {

  private val ports: PortRegistry =
    new PortRegistry

  private val peers: PeerRegistry =
    new PeerRegistry (localId, new StubConnection (_, localId, network))

  private [stubs] def deliver [M] (p: Pickler [M], from: HostId, port: PortId, msg: M): Unit =
    if (!port.isFixed)
      ports.deliver (p, peers.get (from), port, msg)

  def request [Q, A] (desc: RequestDescriptor [Q, A], req: Q, to: StubPeer): Async [A] =
    async { cb =>
      desc.apply (req) (peers.get (to.localId), desc.open (ports) ((rsp, from) => cb (rsp)))
    }}

private object ResponseCaptor {

  def install (random: Random, network: StubNetwork): ResponseCaptor = {
    val c = new ResponseCaptor (random.nextLong) (random, network)
    network.install (c)
    c
  }}
