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

import java.net.SocketAddress
import scala.util.{Random, Try}

import com.treode.async.{Async, Scheduler}
import com.treode.cluster._
import com.treode.pickle.Pickler

import Async.supply

class StubCluster (
    val localId: HostId
) (implicit
    random: Random,
    scheduler: Scheduler,
    network: StubNetwork
) extends Cluster with StubPeer {

  private val ports: PortRegistry =
    new PortRegistry

  private val peers: PeerRegistry =
    new PeerRegistry (localId, new StubConnection (_, localId, network))

  private val scuttlebutt: Scuttlebutt =
    new Scuttlebutt (localId, peers)

  private [stubs] def deliver [M] (p: Pickler [M], from: HostId, port: PortId, msg: M): Unit =
    ports.deliver (p, peers.get (from), port, msg)

  def cellId = CellId (0x0D)

  def listen [M] (desc: MessageDescriptor [M]) (f: (M, Peer) => Any): Unit =
    ports.listen (desc.pmsg, desc.id) (f)

  def listen [Q, A] (desc: RequestDescriptor [Q, A]) (f: (Q, Peer) => Async [A]): Unit =
    desc.listen (ports) (f)

  def listen [M] (desc: RumorDescriptor [M]) (f: (M, Peer) => Any): Unit =
    scuttlebutt.listen (desc) (f)

  def hail (remoteId: HostId, remoteAddr: SocketAddress): Unit =
    peers.get (remoteId) .address = remoteAddr

  def startup() {
    scuttlebutt.attach (this)
    network.install (this)
  }

  def shutdown(): Async [Unit] =
    supply (network.remove (this))

  def peer (id: HostId): Peer =
    peers.get (id)

  def rpeer: Option [Peer] =
    peers.rpeer

  def open [M] (p: Pickler [M]) (f: (M, Peer) => Any): EphemeralPort [M] =
    ports.open (p) (f)

  def open [Q, A] (desc: RequestDescriptor [Q, A]) (f: (Try [A], Peer) => Any): EphemeralPort [Option [A]] =
    desc.open (ports) (f)

  def spread [M] (desc: RumorDescriptor [M]) (msg: M): Unit =
    scuttlebutt.spread (desc) (msg)
}
