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
import java.util.concurrent.Executors
import scala.util.{Random, Try}

import com.treode.async.{Async, Backoff, Scheduler}
import com.treode.async.misc.RichInt
import com.treode.pickle.Pickler

trait Cluster {

  def cellId: CellId

  def localId: HostId

  def listen [M] (desc: MessageDescriptor [M]) (f: (M, Peer) => Any)

  def listen [Q, A] (desc: RequestDescriptor [Q, A]) (f: (Q, Peer) => Async [A])

  def listen [M] (desc: RumorDescriptor [M]) (f: (M, Peer) => Any)

  def hail (remoteId: HostId, remoteAddr: SocketAddress)

  def peer (id: HostId): Peer

  def rpeer: Option [Peer]

  def open [M] (p: Pickler [M]) (f: (M, Peer) => Any): EphemeralPort [M]

  def open [Q, A] (desc: RequestDescriptor [Q, A]) (f: (Try [A], Peer) => Any): EphemeralPort [Option [A]]

  def spread [M] (desc: RumorDescriptor [M]) (msg: M)

  def startup()

  def shutdown(): Async [Unit]
}

object Cluster {

  /** This has been moved to package level for easier access in the Scaladoc. */
  @deprecated ("Use ClusterConfig", "0.3.0")
  type Config = ClusterConfig

  /** This has been moved to package level for easier access in the Scaladoc. */
  @deprecated ("Use ClusterConfig", "0.3.0")
  val Config = ClusterConfig


  def live (
      cellId: CellId,
      hostId: HostId,
      bindAddr: SocketAddress,
      shareAddr: SocketAddress
  ) (implicit
      random: Random,
      scheduler: Scheduler,
      config: ClusterConfig
  ): Cluster = {

    var group: AsynchronousChannelGroup = null

    try {

      val ports = new PortRegistry

      group = AsynchronousChannelGroup.withFixedThreadPool (1, Executors.defaultThreadFactory)

      val peers = PeerRegistry.live (cellId, hostId, group, ports)

      val scuttlebutt = new Scuttlebutt (hostId, peers)

      val listener = new Listener (cellId, hostId, bindAddr, group, peers)

      implicit val cluster  =
        new ClusterLive (cellId, hostId, group, ports, peers, listener, scuttlebutt)

      Peer.address.listen ((addr, peer) => peer.address = addr)
      Peer.address.spread (shareAddr)

      cluster

    } catch {

      case t: Throwable =>
        if (group != null)
          group.shutdownNow()
        throw t
    }}}
