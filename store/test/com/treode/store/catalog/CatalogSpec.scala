package com.treode.store.catalog

import java.util.concurrent.TimeoutException
import scala.util.Random

import com.treode.async.AsyncChecks
import com.treode.cluster.StubNetwork
import com.treode.store.{Bytes, CatalogDescriptor}
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.FreeSpec

import CatalogTestTools._
import StubCatalogHost.cat1

class CatalogSpec extends FreeSpec with AsyncChecks {

  val val0 = Bytes.empty
  val val1 = Bytes (cat1.pcat, 0x9E587C3979DFCFFFL)
  val val2 = Bytes (cat1.pcat, 0x8041608E94F55C6DL)

  private val p1 =
    Patch (0, val0.hashCode, Seq (Patch.diff (val0, val1)))

  private val p2 =
    Patch (0, val0.hashCode, Seq (Patch.diff (val0, val2)))

  private case class Summary (timedout: Boolean, chosen: Set [Update]) {

    def check (domain: Set [Update]) {
      if (intensity == "standard")
        assertResult (domain) (chosen)
    }}

  private def checkConsensus (random: Random, mf: Double, summary: Summary): Summary = {
    val kit = StubNetwork (random)
    import kit.scheduler

    val hs = kit.install (3, new StubCatalogHost (_, kit))
    val Seq (h1, h2, h3) = hs
    for (h <- hs)
      h.setCohorts ((h1, h2, h3))

    try {

      // Propose two patches simultaneously, expect one choice.
      val cb1 = h1.catalogs.propose (cat1.id, p1) .capture()
      val cb2 = h2.catalogs.propose (cat1.id, p2) .capture()
      kit.messageFlakiness = mf
      kit.runTasks (timers = true, count = 500)
      val v = cb1.passed
      assertResult (v) (cb2.passed)

      // Expect all acceptors closed and in agreement.
      val as = hs map (_.acceptors.get (cat1.id))
      assert (as forall (_.isClosed))
      assertResult (1) (as.map (_.getChosen) .flatten.toSet.size)

      Summary (summary.timedout, summary.chosen + v)
    } catch {
      case e: TimeoutException =>
        Summary (true, summary.chosen)
    }}

  "The acceptors should" - {

    "achieve consensus with" - {

      "stable hosts and a reliable network" taggedAs (Intensive, Periodic) in {
        var summary = Summary (false, Set.empty)
        forAllSeeds { random =>
          summary = checkConsensus (random, 0.0, summary)
        }
        summary.check (Set (p1, p2))
      }

      "stable hosts and a flakey network" taggedAs (Intensive, Periodic) in {
        var summary = Summary (false, Set.empty)
        forAllSeeds { random =>
          summary = checkConsensus (random, 0.1, summary)
        }
        summary.check (Set (p1, p2))
      }}}

  "The kit should" - {

    def setup (random: Random = new Random (0)) = {
      val kit = StubNetwork (random)
      val hs = kit.install (3, new StubCatalogHost (_, kit))
      val Seq (h1, h2, h3) = hs
      for (h <- hs)
        h.setCohorts ((h1, h2, h3))
      (kit, hs, h1, h2)
    }

    "distribute a catalog" in {
      forAllSeeds { random =>
        val (kit, hs, h1, _) = setup (random)
        import kit.scheduler
        h1.catalogs.issue (cat1) (1, 0x658C1274DE7CFA8EL) .pass
        scheduler.runTasks (timers = true, count = 500)
        for (h <- hs)
          assertResult (0x658C1274DE7CFA8EL) (h.v1)
      }}

    "reject a new issue when its version number is behind" in {
      val (kit, hs, h1, h2) = setup()
      import kit.scheduler
      h1.catalogs.issue (cat1) (1, 0x658C1274DE7CFA8EL) .pass
      scheduler.runTasks (timers = true, count = 500)
      h2.catalogs.issue (cat1) (1, 0x1195296671067D1AL) .fail [IllegalArgumentException]
      scheduler.runTasks (timers = true, count = 500)
      for (h <- hs)
        assertResult (0x658C1274DE7CFA8EL) (h.v1)
    }

    "reject a new issue when its version number is ahead" in {
      val (kit, hs, h1, h2) = setup()
      import kit.scheduler
      h1.catalogs.issue (cat1) (1, 0x658C1274DE7CFA8EL) .pass
      scheduler.runTasks (timers = true, count = 500)
      h2.catalogs.issue (cat1) (1, 0x1195296671067D1AL) .fail [IllegalArgumentException]
      scheduler.runTasks (timers = true, count = 500)
      for (h <- hs)
        assertResult (0x658C1274DE7CFA8EL) (h.v1)
    }}}
