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

import java.util.ArrayList
import scala.collection.JavaConversions._
import scala.util.Random

import com.treode.async.stubs.{CallbackCaptor, StubScheduler}
import com.treode.cluster.{HostId, MessageDescriptor, PeerRegistry, PortId, Peer}
import com.treode.pickle.Pickler
import org.scalatest.Assertions._

class MessageCaptor (
    val localId: HostId
) (implicit
    random: Random,
    network: StubNetwork
) extends StubPeer {

  private val peers: PeerRegistry =
    new PeerRegistry (localId, new StubConnection (_, localId, network))

  private var msgs = new ArrayList [(PortId, Any, Array [Byte], HostId)]

  private [stubs] def deliver [M] (p: Pickler [M], from: HostId, port: PortId, msg: M): Unit =
    synchronized {
      msgs.add ((port, msg, p.toByteArray (msg), from))
    }

  def expect [M] (desc: MessageDescriptor [M]) (implicit s: StubScheduler): (M, Peer) = {
    if (!msgs.exists (_._1 == desc.id))
      s.run (timers = !msgs.exists (_._1 == desc.id))
    val (port, msg, bytes, from) = synchronized {
      val i = msgs.indexWhere (_._1 == desc.id)
      msgs.remove (math.max (0, i))
    }
    if (desc.id != port)
      fail (s"Expected ${desc.id}, found $port ($msg)")
    (desc.pmsg.fromByteArray (bytes), peers.get (from))
  }

  def expectNone () (implicit s: StubScheduler) {
    s.run (timers = false)
    synchronized {
      if (!msgs.isEmpty) {
        val (port, msg, bytes, from) = msgs.remove (0)
        fail (s"Expected none, found $port ($msg)")
      }}}

  def send [M] (desc: MessageDescriptor [M], to: StubPeer) (msg: M): Unit =
    to.deliver (desc.pmsg, localId, desc.id, msg)

  override def toString =
    msgs.map {case (port, msg, _, from) => (from, port, msg)} .mkString ("\n")
}

object MessageCaptor {

  def install() (implicit random: Random, network: StubNetwork): MessageCaptor = {
    val c = new MessageCaptor (random.nextLong)
    network.install (c)
    c
  }}
