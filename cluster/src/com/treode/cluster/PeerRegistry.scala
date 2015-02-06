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

import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._
import scala.util.Random

import com.treode.async.{Async, Fiber, Scheduler}
import com.treode.async.implicits._

private class PeerRegistry (localId: HostId, newPeer: HostId => Peer) (implicit random: Random) {

  private val peers = new ConcurrentHashMap [Long, Peer]

  def get (id: HostId): Peer = {
    val p0 = peers.get (id)
    if (p0 == null) {
      val p1 = newPeer (id)
      val p2 = peers.putIfAbsent (id.id, p1)
      if (p2 == null) p1 else p2
    } else {
      p0
    }}

  def rpeer: Option [Peer] = {
    if (peers.size > 2) {
      val n = random.nextInt (peers.size - 1)
      val i = peers.valuesIterator.filter (_.id != localId) .drop (n)
      if (i.hasNext) Some (i.next) else None
    } else if (peers.size == 2) {
      val i = peers.valuesIterator.filter (_.id != localId)
      if (i.hasNext) Some (i.next) else None
    } else {
      None
    }}

  def shutdown(): Async [Unit] =
    peers.values.latch (_.close())

  override def toString =
    "PeerRegistry" +
        (peers map (kv => (kv._1, kv._2)) mkString ("(\n    ", ",\n    ", ")"))
}

private object PeerRegistry {

  def live (
      cellId: CellId,
      localId: HostId,
      group: AsynchronousChannelGroup,
      ports: PortRegistry
  ) (implicit
      random: Random,
      scheduler: Scheduler,
      config: Cluster.Config
  ): PeerRegistry = {

    def newPeer (remoteId: HostId): Peer =
      if (remoteId == localId)
        new LocalConnection (localId, ports)
      else
        new RemoteConnection (remoteId, localId, cellId, group, ports)

    new PeerRegistry (localId, newPeer)
  }}
