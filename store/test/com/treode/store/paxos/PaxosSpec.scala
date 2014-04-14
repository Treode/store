package com.treode.store.paxos

import java.util.concurrent.TimeoutException
import scala.util.Random

import com.treode.async.AsyncChecks
import com.treode.cluster.StubNetwork
import com.treode.store.Bytes
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
      kit: StubNetwork,
      p1: StubPaxosHost,         // First host that will submit a proposal.
      p2: StubPaxosHost,         // Second host that will submit a proposal.
      as: Seq [StubPaxosHost],   // Hosts that we expect will accept.
      mf: Double,
      summary: Summary
  ) {
    try {
      import kit.scheduler

      val k = Bytes (0x9E360154E51197A8L)

      val cb1 = p1.paxos.propose (k, 1) .capture()
      val cb2 = p2.paxos.propose (k, 2) .capture()
      kit.messageFlakiness = mf
      kit.runTasks (timers = true, count = 1000)
      val v = cb1.passed
      assertResult (v) (cb2.passed)

      for (a <- as)
        assert (
            !a.paxos.acceptors.acceptors.contains (k),
            "Expected acceptor to have been removed.")
      for (a <- as)
        a.paxos.archive.get (k) .expect (Some (v))

      summary.chose (v.int)
    } catch {
      case e: TimeoutException =>
        summary.timedout = true
    }}

  "The acceptors should" - {

    "achieve consensus with" - {

      "stable hosts and a reliable network" taggedAs (Intensive, Periodic) in {
        var summary = new Summary
        forAllSeeds { random =>
          val kit = StubNetwork (random)
          val hs = kit.install (3, new StubPaxosHost (_, kit))
          val Seq (h1, h2, h3) = hs
          for (h <- hs)
            h.setCohorts (settled (0, h1, h2, h3))
          check (kit, h1, h2, hs, 0.0, summary)
        }
        summary.check (Set (1, 2))
      }

      "stable hosts and a flakey network" taggedAs (Intensive, Periodic) in {
        var summary = new Summary
        forAllSeeds { random =>
          val kit = StubNetwork (random)
          val hs = kit.install (3, new StubPaxosHost (_, kit))
          val Seq (h1, h2, h3) = hs
          for (h <- hs)
            h.setCohorts (settled (0, h1, h2, h3))
          check (kit, h1, h2, hs, 0.1, summary)
        }
        summary.check (Set (1, 2))
      }

      "atlas distributed by catalogs" in {
        var summary = new Summary
        forAllSeeds { random =>
          val kit = StubNetwork (random)
          import kit.scheduler

          val hs = kit.install (3, new StubPaxosHost (_, kit))
          val Seq (h1, h2, h3) = hs
          for (h1 <- hs; h2 <- hs)
            h1.hail (h2.localId, null)
          h1.setCohorts (settled (0, h1, h2, h3))
          h1.issueCohorts (settled (0, h1, h2, h3)) .pass
          kit.runTasks (timers = true, count = 500)

          check (kit, h1, h2, hs, 0.1, summary)
        }
        summary.check (Set (1, 2))
      }}

    "rebalance" in { pending
      val kit = StubNetwork()
      import kit.scheduler

      val hs = kit.install (4, new StubPaxosHost (_, kit))
      val Seq (h1, h2, h3, h4) = hs
      for (h <- hs)
        h.setCohorts (settled (0, h1, h2, h3))

      val k = Bytes (0xB3334572873016E4L)
      h1.paxos.propose (k, 1) .expect (1)

      for (h <- hs)
        h.setCohorts (moving (0, (h1, h2, h3), (h1, h2, h4)))

      for (h <- hs)
        h.paxos.archive.get (k) .expect (Some (1))
    }}}
