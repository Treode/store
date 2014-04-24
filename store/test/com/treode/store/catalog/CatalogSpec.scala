package com.treode.store.catalog

import java.util.concurrent.TimeoutException
import scala.util.Random

import com.treode.async.AsyncChecks
import com.treode.cluster.StubNetwork
import com.treode.store.{Bytes, CatalogDescriptor, StaleException}
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.FreeSpec

import CatalogTestTools._
import StubCatalogHost.cat1

class CatalogSpec extends FreeSpec with AsyncChecks {

  val val0 = Bytes.empty
  val val1 = Bytes (cat1.pcat, 0x9E587C3979DFCFFFL)
  val val2 = Bytes (cat1.pcat, 0x8041608E94F55C6DL)

  private val patch1 =
    Patch (0, val1.murmur32, Seq (Patch.diff (val0, val1)))

  private val patch2 =
    Patch (0, val2.murmur32, Seq (Patch.diff (val0, val2)))

  private class Summary (var timedout: Boolean, var chosen: Set [Update]) {

    def this() = this (false, Set.empty)

    def chose (v: Update): Unit =
      chosen += v

    def check (domain: Set [Update]) {
      if (intensity == "standard")
        assert (domain forall (chosen contains _))
      val domain0 = domain + Update.empty
      assert (chosen forall (domain0 contains _))
    }}

  // Propose two patches simultaneously, expect one choice.
  def check (
      kit: StubNetwork,
      p1: StubCatalogHost,         // First host that will submit a proposal.
      p2: StubCatalogHost,         // Second host that will submit a proposal.
      as: Seq [StubCatalogHost],   // Hosts that we expect will accept.
      mf: Double,
      summary: Summary
  ) {
    try {

      val cb1 = p1.catalogs.propose (cat1.id, patch1) .capture()
      val cb2 = p2.catalogs.propose (cat1.id, patch2) .capture()
      kit.messageFlakiness = mf
      kit.runTasks (timers = true, count = 500)
      val v = cb1.passed
      assertResult (v) (cb2.passed)

      for (h <- as)
        assert (
            !h.catalogs.acceptors.acceptors.contains (cat1.id),
            "Expected acceptor to have been removed.")

      summary.chose (v)
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
          val hs = kit.install (3, new StubCatalogHost (_, kit))
          val Seq (h1, h2, h3) = hs
          for (h <- hs)
            h.setAtlas (settled (h1, h2, h3))
          check (kit, h1, h2, hs, 0.0, summary)
        }
        summary.check (Set (patch1, patch2))
      }

      "stable hosts and a flakey network" taggedAs (Intensive, Periodic) in {
        var summary = new Summary
        forAllSeeds { random =>
          val kit = StubNetwork (random)
          val hs = kit.install (3, new StubCatalogHost (_, kit))
          val Seq (h1, h2, h3) = hs
          for (h <- hs)
            h.setAtlas (settled (h1, h2, h3))
          check (kit, h1, h2, hs, 0.1, summary)
        }
        summary.check (Set (patch1, patch2))
      }}}

  "The kit should" - {

    def setup (random: Random = new Random (0)) = {
      val kit = StubNetwork (random)
      val hs = kit.install (3, new StubCatalogHost (_, kit))
      val Seq (h1, h2, h3) = hs
      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))
      (kit, hs, h1, h2)
    }

    "distribute one issue of a catalog" in {
      forAllSeeds { random =>
        val (kit, hs, h1, _) = setup (random)
        import kit.scheduler
        h1.catalogs.issue (cat1) (1, 0x658C1274DE7CFA8EL) .pass
        scheduler.runTasks (timers = true, count = 500)
        for (h <- hs)
          assertResult (0x658C1274DE7CFA8EL) (h.v1)
      }}

    "distribute two issues of a catalog, one after the other" in {
      forAllSeeds { random =>
        val (kit, hs, h1, _) = setup (random)
        import kit.scheduler
        h1.catalogs.issue (cat1) (1, 0x658C1274DE7CFA8EL) .pass
        h1.catalogs.issue (cat1) (2, 0x48B944DD188FD6D1L) .pass
        scheduler.runTasks (timers = true, count = 500)
        for (h <- hs)
          assertResult (0x48B944DD188FD6D1L) (h.v1)
      }}

    "reject a new issue when its version number is behind" in {
      val (kit, hs, h1, h2) = setup()
      import kit.scheduler
      h1.catalogs.issue (cat1) (1, 0x658C1274DE7CFA8EL) .pass
      scheduler.runTasks (timers = true, count = 500)
      h2.catalogs.issue (cat1) (1, 0x1195296671067D1AL) .fail [StaleException]
      scheduler.runTasks (timers = true, count = 500)
      for (h <- hs)
        assertResult (0x658C1274DE7CFA8EL) (h.v1)
    }

    "reject a new issue when its version number is ahead" in {
      val (kit, hs, h1, h2) = setup()
      import kit.scheduler
      h1.catalogs.issue (cat1) (1, 0x658C1274DE7CFA8EL) .pass
      scheduler.runTasks (timers = true, count = 500)
      h2.catalogs.issue (cat1) (1, 0x1195296671067D1AL) .fail [StaleException]
      scheduler.runTasks (timers = true, count = 500)
      for (h <- hs)
        assertResult (0x658C1274DE7CFA8EL) (h.v1)
    }}}
