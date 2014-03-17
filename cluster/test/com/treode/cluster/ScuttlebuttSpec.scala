package com.treode.cluster

import scala.util.Random

import com.treode.async.{AsyncTestTools, StubScheduler}
import com.treode.async.io.Socket
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{Pickler, Picklers, PicklerRegistry}
import org.scalacheck.Gen
import org.scalatest.{FreeSpec, PropSpec, Suites}
import org.scalatest.prop.PropertyChecks

import AsyncTestTools._
import PicklerRegistry.{BaseTag, FunctionTag}
import Scuttlebutt.{Handler, Sync}

class ScuttlebuttSpec extends Suites (ScuttlebuttBehaviors, ScuttlebuttProperties)

object ScuttlebuttBehaviors extends FreeSpec {

  val LOCAL = HostId (0x23)
  val PEER1 = HostId (0x52)
  val PEER2 = HostId (0x28)

  val rumor = RumorDescriptor (0x63, Picklers.int)

  def assertSeq [T] (xs: T*) (test: => Seq [T]): Unit =
    assertResult (xs) (test)

  def tag [M] (desc: RumorDescriptor [M]) (msg: M): Handler = {
    new BaseTag (desc.pmsg, desc.id.id, msg) with FunctionTag [Peer, Any] {
      def apply (from: Peer) = throw new IllegalArgumentException
      override def toString: String = f"Tag($id%X,$msg)"
    }}

  def delta (host: HostId, deltas: (Int, Int)*) = {
    val _deltas =
      for ((msg, vers) <- deltas)
        yield (tag (rumor) (msg) -> vers)
    host -> _deltas
  }

  class StubPeer (val id: HostId) extends Peer{

    def connect (socket: Socket, input: PagedBuffer, clientId: HostId) = ???
    def close() = ???
    def send [A] (p: Pickler [A], port: PortId, msg: A) = ???
  }

  class RichScuttlebutt (implicit random: Random, scheduler: StubScheduler) {

    val peers = new PeerRegistry (LOCAL, new StubPeer (_))

    val sb = new Scuttlebutt (LOCAL, peers)

    def listen (f: (Int, Peer) => Any) =
      sb.listen (rumor) (f)

    def status = sb.status.pass

    def spread (v: Int) = {
      sb.spread (rumor) (v)
      scheduler.runTasks()
    }

    def ping (hosts: (HostId, Int)*) =
      sb.ping (hosts) .pass

    def delta (host: HostId, deltas: (Int, Int)*) = {
      val _deltas =
        for ((msg, vers) <- deltas)
          yield (sb.loopback (rumor) (msg) -> vers)
      host -> _deltas
    }

    def sync (updates: (HostId, Seq [(Handler, Int)])*) {
      sb.sync (updates)
      scheduler.runTasks()
    }}

  def mkScuttlebutt = {
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
      assertSeq (delta (LOCAL, (1, 1))) (sb.ping())
    }


    "yield non-empty deltas on ping missing this host" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      sb.listen ((_v, from) => ())
      sb.spread (1)
      assertSeq (delta (LOCAL, (1, 1))) (sb.ping (PEER1 -> 1))
    }

    "yield non-empty deltas on ping out-of-date with this host" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      sb.listen ((_v, from) => ())
      sb.spread (1)
      assertSeq (delta (LOCAL, (1, 1))) (sb.ping (LOCAL -> 0))
    }

    "yield empty deltas on ping up-to-date with this host" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      sb.spread (1)
      assertSeq () (sb.ping (LOCAL -> 1))
    }}

  "When Scuttlebutt receives a sync with one peer it should" - {

    "yield a status that contains the peer" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      sb.sync (sb.delta (PEER1, (1, 1)))
      assertSeq (PEER1 -> 1) (sb.status)
    }

    "raise the local version number" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      sb.sync (sb.delta (PEER1, (1, 3745)))
      sb.spread (2)
      assertSeq (PEER1 -> 3745, LOCAL -> 3746) (sb.status)
    }

    "invoke the listener on a first update" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      var v = 0
      sb.listen ((_v, from) => v = _v)
      sb.sync (sb.delta (PEER1, (1, 1)))
      assertResult (1) (v)
    }

    "invoke the listener on second update" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      var v = 0
      sb.listen ((_v, from) => v = _v)
      sb.sync (sb.delta (PEER1, (1, 1)))
      sb.sync (sb.delta (PEER1, (2, 2)))
      assertResult (2) (v)
    }

    "ignore a repeated update" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      var count = 0
      sb.listen ((v, from) => count += 1)
      sb.sync (sb.delta (PEER1, (1, 1)))
      sb.sync (sb.delta (PEER1, (1, 1)))
      assertResult (1) (count)
    }}

  "When Scuttlebutt receives a sync with two peers it should" - {

    "invoke the listener once foreach peer" in {
      implicit val (scheduler, sb) = mkScuttlebutt
      var vs = Map.empty [HostId, Int]
      sb.listen ((v, from) => vs += from.id -> v)
      sb.sync (sb.delta (PEER1, (1, 1)), sb.delta (PEER2, (2, 1)))
      assertResult (Map (PEER1 -> 1, PEER2 -> 2)) (vs)
    }}}

object ScuttlebuttProperties extends PropSpec with PropertyChecks {

  val seeds = Gen.choose (0L, Long.MaxValue)

  val r1 = RumorDescriptor (0x15, Picklers.int)
  val r2 = RumorDescriptor (0xEF, Picklers.int)
  val r3 = RumorDescriptor (0x6C, Picklers.int)

  // Avoid inliner warning.
  def entry [M] (host: HostId, rumor: RumorDescriptor [M], msg: M) =
    ((host, rumor.id), msg)

  class StubHost (id: HostId, network: StubNetwork) extends StubActiveHost (id, network) {

    implicit val cluster: Cluster = this

    var heard = Map.empty [(HostId, RumorId), Int]

    r1.listen ((v, from) => heard += entry (from.id, r1, v))
    r2.listen ((v, from) => heard += entry (from.id, r2, v))
    r3.listen ((v, from) => heard += entry (from.id, r3, v))

    scuttlebutt.attach (this)
  }

  def checkUnity (seed: Long, mf: Double) {
    val kit = StubNetwork (seed)
    kit.messageFlakiness = mf
    val hs = kit.install (3, new StubHost (_, kit))
    for (h1 <- hs; h2 <- hs)
      h1.hail (h2.localId, null)
    kit.runTasks()

    val Seq (h1, h2, h3) = hs
    h1.spread (r1) (1)
    h2.spread (r2) (2)
    h2.spread (r2) (3)
    h3.spread (r2) (4)
    h3.spread (r3) (5)
    h3.spread (r3) (6)
    kit.runTasks (timers = true, count = 200)

    // Hosts do not hear their own rumors.
    h1.heard += entry (h1.localId, r1, 1)
    h2.heard += entry (h2.localId, r2, 3)
    h3.heard += entry (h3.localId, r2, 4)
    h3.heard += entry (h3.localId, r3, 6)

    val expected = Map (
        entry (h1.localId, r1, 1),
        entry (h2.localId, r2, 3),
        entry (h3.localId, r2, 4),
        entry (h3.localId, r3, 6))
    assert (hs forall (_.heard == expected))
  }

  property ("Scuttlebutt should spread rumors") {
    forAll (seeds) { seed =>
      checkUnity (seed, 0.0)
    }}

  property ("Scuttlebutt should spread rumors with a flakey network") {
    forAll (seeds) { seed =>
      checkUnity (seed, 0.1)
    }}}
