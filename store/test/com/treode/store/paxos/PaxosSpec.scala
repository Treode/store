package com.treode.store.paxos

import java.util.concurrent.TimeoutException
import scala.util.Random

import com.treode.async.stubs.AsyncChecks
import com.treode.async.stubs.implicits._
import com.treode.cluster.stubs.StubNetwork
import com.treode.store.{Bytes, Cell, StoreTestKit}
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.FreeSpec

import PaxosTestTools._

class PaxosSpec extends FreeSpec with AsyncChecks {

  class Summary (var timedout: Boolean, var chosen: Set [Int]) {

    def this() = this (false, Set.empty)

    def chose (v: Int): Unit =
      chosen += v

    def check (domain: Set [Int]) {
      if (intensity == "standard")
        assertResult (domain) (chosen)
    }}

  // Propose two values simultaneously, expect one choice.
  def check (
      kit: StoreTestKit,
      p1: StubPaxosHost,         // First host that will submit a proposal.
      p2: StubPaxosHost,         // Second host that will submit a proposal.
      as: Seq [StubPaxosHost],   // Hosts that we expect will accept.
      mf: Double,
      summary: Summary
  ) {
    try {
      import kit._

      val k = Bytes (0x9E360154E51197A8L)

      val cb1 = p1.paxos.propose (k, 0, 1) .capture()
      val cb2 = p2.paxos.propose (k, 0, 2) .capture()
      kit.messageFlakiness = mf
      kit.run (timers = true, count = 1000)
      val v = cb1.passed
      assertResult (v) (cb2.passed)

      for (a <- as)
        assert (
            !a.paxos.acceptors.acceptors.contains (k),
            "Expected acceptor to have been removed.")
      for (a <- as)
        a.paxos.archive.get (k, 0) .expect (k##0::v)

      summary.chose (v.int)
    } catch {
      case e: TimeoutException =>
        summary.timedout = true
    }}

  "The paxos implementation should" - {

    "achieve consensus with" - {

      "stable hosts and a reliable network" taggedAs (Intensive, Periodic) in {
        var summary = new Summary
        forAllSeeds { implicit random =>
          implicit val kit = StoreTestKit.random (random)
          import kit.{network, scheduler}
          val hs = Seq.fill (3) (StubPaxosHost .install() .pass)
          val Seq (h1, h2, h3) = hs
          for (h <- hs)
            h.setAtlas (settled (h1, h2, h3))
          check (kit, h1, h2, hs, 0.0, summary)
        }
        summary.check (Set (1, 2))
      }

      "stable hosts and a flakey network" taggedAs (Intensive, Periodic) in {
        var summary = new Summary
        forAllSeeds { implicit random =>
          implicit val kit = StoreTestKit.random (random)
          import kit.{network, scheduler}
          val hs = Seq.fill (3) (StubPaxosHost .install() .pass)
          val Seq (h1, h2, h3) = hs
          for (h <- hs)
            h.setAtlas (settled (h1, h2, h3))
          check (kit, h1, h2, hs, 0.1, summary)
        }
        summary.check (Set (1, 2))
      }}

    "rebalance" in {
      implicit val kit = StoreTestKit.random()
      import kit.{network, random, scheduler}

      val hs = Seq.fill (4) (StubPaxosHost .install() .pass)
      val Seq (h1, h2, h3, h4) = hs
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      h1.issueAtlas (settled (h1, h2, h3)) .pass
      h1.issueAtlas (moving (h1, h2, h3) (h1, h2, h4)) .pass

      val k = Bytes (0xB3334572873016E4L)
      h1.paxos.propose (k, 0, 1) .expect (1)

      for (h <- hs)
        h.paxos.archive.get (k, 0) .expect (k##0::1)
      kit.run (count = 1000, timers = true)
      expectAtlas (3, settled (h1, h2, h4)) (hs)
      for (h <- Seq (h1, h2, h4))
        h.paxos.archive.get (k, 0) .expect (k##0::1)
      // TierTable.get does not account for residents.
      // h3.paxos.archive.get (k, 0) .expect (k##0)
    }}}
