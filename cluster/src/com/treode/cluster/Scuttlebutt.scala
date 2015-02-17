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

import com.treode.async.{Async, Callback, Fiber, Scheduler}
import com.treode.buffer.ArrayBuffer
import com.treode.pickle.{InvalidTagException, Pickler, PicklerRegistry}

import Callback.ignore
import Scuttlebutt.{Handler, Ping, Sync, Universe}

private class Scuttlebutt (localId: HostId, peers: PeerRegistry) (implicit scheduler: Scheduler) {

  private val fiber = new Fiber
  private val localHost = peers.get (localId)
  private var universe: Universe = Map.empty .withDefaultValue (Map.empty)
  private var next = 1

  private val ports = PicklerRegistry [Handler] { id: Long => from: Peer => () }

  def loopback [M] (desc: RumorDescriptor [M], msg: M): Handler =
    ports.loopback (desc.pmsg, desc.id.id, msg)

  def unpickle (id: RumorId, msg: Array [Byte]): Handler =
    ports.unpickle (id.id, ArrayBuffer.readable (msg))

  def listen [M] (desc: RumorDescriptor [M]) (f: (M, Peer) => Any): Unit =
    fiber.execute {
      ports.register (desc.pmsg, desc.id.id) (f.curried)
      for {
        (host, state) <- universe
        (msg, _) <- state get (desc.id)
      } {
        scheduler.execute (unpickle (desc.id, msg) (peers.get (host)))
      }}

  def spread [M] (desc: RumorDescriptor [M]) (msg: M): Unit =
    fiber.execute {
      val value = desc.pmsg.toByteArray (msg)
      universe += (localId -> (universe (localId) + (desc.id -> ((value, next)))))
      next += 1
      scheduler.execute (loopback (desc, msg) (localHost))
    }

  private def _status: Ping =
    for ((host, values) <- universe.toSeq)
      yield (host -> values.map (_._2._2) .max)

  def status: Async [Ping] =
    fiber.supply (_status)

  def ping (hosts: Ping): Async [Sync] =
    fiber.supply {
      val _hosts = hosts.toMap.withDefaultValue (0)
      for {
        (host, state) <- universe.toSeq
        n = _hosts (host)
        deltas =
          (for ((id, (v1, n1)) <- state; if n1 > n) yield (id, v1, n1)) .toSeq
        if !deltas.isEmpty
      } yield (host -> deltas)
    }

  def ping (peer: Peer): Unit =
    fiber.execute {
      Scuttlebutt.ping (_status) (peer)
    }

  def sync (updates: Sync): Unit =
    fiber.execute {
      for ((host, deltas) <- updates) {
        val peer = peers.get (host)
        var state = universe (host)
        for ((id, v1, n1) <- deltas) {
          val v0 = state.get (id)
          if (v0.isEmpty || v0.get._2 < n1) {
            state += id -> (v1, n1)
            if (next < n1) next = n1 + 1
            scheduler.execute (unpickle (id, v1) (peer))
          }}
        universe += host -> state
      }}

  def gab() {
    scheduler.delay (200) {
      peers.rpeer match {
        case Some (peer) => ping (peer)
        case None => ()
      }
      gab()
    }}

  def attach (implicit cluster: Cluster) {

    Scuttlebutt.ping.listen { (hosts, from) =>
      val task = for {
        updates <- ping (hosts)
        if !updates.isEmpty
      } yield Scuttlebutt.sync (updates) (from)
      task run (ignore)
    }

    Scuttlebutt.sync.listen { (updates, from) =>
      sync (updates)
    }

    gab()
  }}

private object Scuttlebutt {

  type Handler = Peer => Any
  type Value = Array [Byte]
  type Ping = Seq [(HostId, Int)]
  type Sync = Seq [(HostId, Seq [(RumorId, Value, Int)])]
  type Universe = Map [HostId, Map [RumorId, (Value, Int)]]

  val ping: MessageDescriptor [Ping] = {
    import ClusterPicklers._
    MessageDescriptor (
        0xFF30F8E94A893997L,
        seq (tuple (hostId, uint)))
  }

  val sync: MessageDescriptor [Sync] = {
    import ClusterPicklers._
    MessageDescriptor (
        0xFF3FBB2A507B2F73L,
        seq (tuple (hostId, seq (tuple (rumorId, array (byte), uint)))))
  }}
