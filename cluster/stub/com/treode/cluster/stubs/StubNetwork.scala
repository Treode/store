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

import java.util.concurrent.{ConcurrentHashMap, Executors}
import scala.util.Random

import com.treode.async.stubs.StubScheduler
import com.treode.cluster.{HostId, PortId}
import com.treode.pickle.Pickler

import StubNetwork.inactive

class StubNetwork private (implicit random: Random) {

  private val peers = new ConcurrentHashMap [HostId, StubPeer]

  var messageFlakiness = 0.0
  var messageTrace = false

  private [stubs] def install (peer: StubPeer) {
    if (peers.putIfAbsent (peer.localId, peer) != null &&
        !peers.replace (peer.localId, inactive, peer))
      throw new IllegalArgumentException (s"Host ${peer.localId} is already installed.")
  }

  private [stubs] def remove (peer: StubPeer) {
    if (!peers.replace (peer.localId, peer, inactive) &&
        peers.get (peer.localId) != inactive)
      throw new IllegalArgumentException (s"Host ${peer.localId} was rebooted.")
  }

  private [stubs] def deliver [M] (p: Pickler [M], from: HostId, to: HostId, port: PortId, msg: M) {
    if (messageFlakiness > 0.0 && random.nextDouble < messageFlakiness)
      return
    val h = peers.get (to)
    require (h != null, s"Host $to does not exist.")
    if (messageTrace)
      println (s"$from->$to:$port: $msg $h")
    h.deliver (p, from, port, msg)
  }

  def active (id: HostId): Boolean = {
    val p = peers.get (id)
    p != null && p != inactive
  }}

object StubNetwork {

  private [stubs] val inactive =
    new StubPeer {
      val localId = HostId (0)
      def deliver [M] (p: Pickler [M], from: HostId, port: PortId, msg: M) = ()
    }

  def apply (random: Random): StubNetwork =
    new StubNetwork () (random)
}
