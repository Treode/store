package com.treode.store.catalog

import java.util.concurrent.TimeoutException
import scala.util.Random

import com.treode.async.AsyncChecks
import com.treode.cluster.StubNetwork
import com.treode.store.{Bytes, CatalogDescriptor}
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.FreeSpec

import CatalogTestTools._

class CatalogSpec extends FreeSpec with AsyncChecks {

  val desc = CatalogDescriptor (0x69A94EB07639DF94L, CatalogPicklers.uint)

  val cat0 = Bytes.empty
  val cat1 = Bytes (desc.pcat, 1)
  val cat2 = Bytes (desc.pcat, 2)

  private val p1 =
    Patch (0, cat0.hashCode, Seq (Patch.diff (cat0, cat1)))

  private val p2 =
    Patch (0, cat0.hashCode, Seq (Patch.diff (cat0, cat2)))

  private case class Summary (timedout: Boolean, chosen: Set [Update]) {

    def check (domain: Set [Update]) {
      if (intensity == "standard")
        assertResult (domain) (chosen)
    }}

  private def checkConsensus (random: Random, mf: Double, summary: Summary): Summary = {
    val kit = StubNetwork (random)
    val hs = kit.install (3, new StubCatalogHost (_, kit))
    val Seq (h1, h2, h3) = hs

    for (h <- hs)
      h.setCohorts ((h1, h2, h3))

    import kit.scheduler

    try {

      // Propose two patches simultaneously, expect one choice.
      val cb1 = h1.catalogs.propose (desc.id, p1) .capture()
      val cb2 = h2.catalogs.propose (desc.id, p2) .capture()
      kit.messageFlakiness = mf
      kit.runTasks (timers = true, count = 500)
      val v = cb1.passed
      assertResult (v) (cb2.passed)

      // Expect all acceptors closed and in agreement.
      val as = hs map (_.acceptors.get (desc.id))
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
    }
  }
}
