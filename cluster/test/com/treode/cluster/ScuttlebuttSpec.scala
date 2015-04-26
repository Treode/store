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

import java.util.Arrays
import scala.util.Random

import com.treode.async.Scheduler
import com.treode.async.io.Socket
import com.treode.async.stubs.{AsyncChecks, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.buffer.PagedBuffer
import com.treode.cluster.stubs.{StubCluster, StubNetwork}
import com.treode.pickle.{Pickler, Picklers, PicklerRegistry}
import org.scalatest.{FreeSpec, PropSpec, Suites}

import Scuttlebutt.{Sync, Value}

class ScuttlebuttSpec extends Suites (ScuttlebuttBehaviors, new ScuttlebuttProperties)

object ScuttlebuttBehaviors extends FreeSpec {

  val LOCAL = HostId (0x23)
  val PEER1 = HostId (0x52)
  val PEER2 = HostId (0x28)

  val rumor = RumorDescriptor (0x63, Picklers.int)

  def assertSeq [T] (xs: T*) (test: => Seq [T]): Unit =
    assertResult (xs) (test)

  def assertSync [T] (xs: (HostId, Seq [(RumorId, Value, Int)])*) (test: => Sync): Unit = {
    def f (xs: Seq [(RumorId, Value, Int)]) =
      for ((id, v, n) <- xs) yield (id, Arrays.hashCode (v), n)
    def g (x: (HostId, Seq [(RumorId, Value, Int)])) = (x._1, f (x._2))
    assertResult (xs map (g _)) (test map (g _))
  }

  def delta (host: HostId, deltas: (Int, Int)*) = {
    val _deltas =
      for ((msg, vers) <- deltas)
        yield (rumor.id, rumor.pmsg.toByteArray (msg), vers)
    host -> _deltas
  }

  class StubPeer (val id: HostId) extends Peer{

    def connect (socket: Socket, input: PagedBuffer, clientId: HostId) = ???
    def close() = ???
    def send [A] (p: Pickler [A], port: PortId, msg: A) = ???
  }

  private class RichScuttlebutt (implicit random: Random, scheduler: StubScheduler) {

    val peers = new PeerRegistry (LOCAL, new StubPeer (_))

    val sb = new Scuttlebutt (LOCAL, peers)

    def listen (f: (Int, Peer) => Any) =
      sb.listen (rumor) (f)

    def status = sb.status.expectPass()

    def spread (v: Int) = {
      sb.spread (rumor) (v)
      scheduler.run()
    }

    def ping (hosts: (HostId, Int)*) =
      sb.ping (hosts) .expectPass()

    def sync (updates: (HostId, Seq [(RumorId, Value, Int)])*) {
      sb.sync (updates)
      scheduler.run()
    }}

  private def mkScuttlebutt = {
    implicit val random = new Random (0)
    implicit val scheduler = StubScheduler.random (random)
    val sb = new RichScuttlebutt
    (scheduler, sb)
  }

  "When Scuttlebutt has no values it should" - {

    "yield an empty status" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      assertSeq () (sb.status)
    }

    "yield empty deltas on empty ping" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      assertSeq () (sb.ping())
    }

    "yield empty deltas on non-empty ping" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      assertSeq () (sb.ping (PEER1 -> 1))
    }}

  "When Scuttlebutt has one local update it should" - {

    "yield a status containg LOCAL" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      sb.spread (1)
      assertSeq (LOCAL -> 1) (sb.status)
    }

    "raise the version number" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      sb.spread (1)
      sb.spread (2)
      assertSeq (LOCAL -> 2) (sb.status)
    }

    "yield non-empty deltas on empty ping" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      sb.listen ((_v, from) => ())
      sb.spread (1)
      assertSync (delta (LOCAL, (1, 1))) (sb.ping())
    }

    "yield non-empty deltas on ping missing this host" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      sb.listen ((_v, from) => ())
      sb.spread (1)
      assertSync (delta (LOCAL, (1, 1))) (sb.ping (PEER1 -> 1))
    }

    "yield non-empty deltas on ping out-of-date with this host" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      sb.listen ((_v, from) => ())
      sb.spread (1)
      assertSync (delta (LOCAL, (1, 1))) (sb.ping (LOCAL -> 0))
    }

    "yield empty deltas on ping up-to-date with this host" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      sb.spread (1)
      assertSeq () (sb.ping (LOCAL -> 1))
    }}

  "When Scuttlebutt receives a sync with one peer it should" - {

    "yield a status that contains the peer" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      sb.sync (delta (PEER1, (1, 1)))
      assertSeq (PEER1 -> 1) (sb.status)
    }

    "raise the local version number" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      sb.sync (delta (PEER1, (1, 3745)))
      sb.spread (2)
      assertSeq (PEER1 -> 3745, LOCAL -> 3746) (sb.status)
    }

    "invoke the listener on a first update" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      var v = 0
      sb.listen ((_v, from) => v = _v)
      sb.sync (delta (PEER1, (1, 1)))
      assertResult (1) (v)
    }

    "invoke the listener on second update" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      var v = 0
      sb.listen ((_v, from) => v = _v)
      sb.sync (delta (PEER1, (1, 1)))
      sb.sync (delta (PEER1, (2, 2)))
      assertResult (2) (v)
    }

    "ignore a repeated update" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      var count = 0
      sb.listen ((v, from) => count += 1)
      sb.sync (delta (PEER1, (1, 1)))
      sb.sync (delta (PEER1, (1, 1)))
      assertResult (1) (count)
    }}

  "When Scuttlebutt receives a sync with two peers it should" - {

    "invoke the listener once foreach peer" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      var vs = Map.empty [HostId, Int]
      sb.listen ((v, from) => vs += from.id -> v)
      sb.sync (delta (PEER1, (1, 1)), delta (PEER2, (2, 1)))
      assertResult (Map (PEER1 -> 1, PEER2 -> 2)) (vs)
    }}}

class ScuttlebuttProperties extends PropSpec with AsyncChecks {

  val r1 = RumorDescriptor (0x15, Picklers.int)
  val r2 = RumorDescriptor (0xEF, Picklers.int)
  val r3 = RumorDescriptor (0x6C, Picklers.int)

  // Avoid inliner warning.
  def entry [M] (host: HostId, rumor: RumorDescriptor [M], msg: M) =
    ((host, rumor.id), msg)

  class StubHost (
      val localId: HostId
   ) (implicit
       random: Random,
       scheduler: Scheduler,
       network: StubNetwork
   ) {

    implicit val cluster = new StubCluster (localId)

    var heard = Map.empty [(HostId, RumorId), Int]

    r1.listen ((v, from) => heard += entry (from.id, r1, v))
    r2.listen ((v, from) => heard += entry (from.id, r2, v))
    r3.listen ((v, from) => heard += entry (from.id, r3, v))

    cluster.startup()

    def hail (remoteId: HostId): Unit =
      cluster.hail (remoteId, null)

    def spread [M] (desc: RumorDescriptor [M]) (msg: M): Unit =
      cluster.spread (desc) (msg)
  }

  def checkUnity (mf: Double) (implicit random: Random) {
    implicit val scheduler = StubScheduler.random (random)
    implicit val network = StubNetwork (random)
    network.messageFlakiness = mf
    val hs = Seq.fill (3) (new StubHost (random.nextLong))
    for (h1 <- hs; h2 <- hs)
      h1.hail (h2.localId)
    scheduler.run()

    val Seq (h1, h2, h3) = hs
    h1.spread (r1) (1)
    h2.spread (r2) (2)
    scheduler.run()
    h2.spread (r2) (3)
    h3.spread (r2) (4)
    h3.spread (r3) (5)
    scheduler.run()
    h3.spread (r3) (6)

    val expected = Map (
        entry (h1.localId, r1, 1),
        entry (h2.localId, r2, 3),
        entry (h3.localId, r2, 4),
        entry (h3.localId, r3, 6))
    val count = scheduler.run (
        timers = hs forall (_.heard == expected),
        count = 1000)
    assert (count < 1000)
  }

  property ("Scuttlebutt should spread rumors") {
    forAllRandoms { implicit random =>
      checkUnity (0.0) (random)
    }}

  property ("Scuttlebutt should spread rumors with a flakey network") {
    forAllRandoms { random =>
      checkUnity (0.1) (random)
    }}}
