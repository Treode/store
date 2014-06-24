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

package com.treode.cluster

import java.net.SocketAddress
import java.nio.channels.AsynchronousChannelGroup

import com.treode.async.Async
import com.treode.pickle.Pickler

import Async.guard

private class ClusterLive (
    val cellId: CellId,
    val localId: HostId,
    group: AsynchronousChannelGroup,
    ports: PortRegistry,
    peers: PeerRegistry,
    listener: Listener,
    scuttlebutt: Scuttlebutt
) extends Cluster {

  def listen [M] (desc: MessageDescriptor [M]) (f: (M, Peer) => Any): Unit =
    ports.listen (desc.pmsg, desc.id) (f)

  def listen [M] (desc: RumorDescriptor [M]) (f: (M, Peer) => Any): Unit =
    scuttlebutt.listen (desc) (f)

  def hail (remoteId: HostId, remoteAddr: SocketAddress): Unit =
    peers.get (remoteId) .address = remoteAddr

  def startup(): Unit = {
    scuttlebutt.attach (this)
    listener.startup()
  }

  def shutdown(): Async [Unit] =
    guard {
      listener.shutdown()
      for {
        _ <- peers.shutdown()
      } yield {
        group.shutdownNow()
      }}

  def peer (id: HostId): Peer =
    peers.get (id)

  def rpeer: Option [Peer] =
    peers.rpeer

  def open [M] (p: Pickler [M]) (f: (M, Peer) => Any): EphemeralPort [M] =
    ports.open (p) (f)

  def spread [M] (desc: RumorDescriptor [M]) (msg: M): Unit =
    scuttlebutt.spread (desc) (msg)
}
