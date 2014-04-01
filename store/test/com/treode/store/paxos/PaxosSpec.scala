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

  case class Summary (timedout: Boolean, chosen: Set [Int]) {

    def check (domain: Set [Int]) {
      if (intensity == "standard")
        assertResult (domain) (chosen)
    }}

  def checkConsensus (random: Random, mf: Double, summary: Summary): Summary = {
    val kit = StubNetwork (random)
    val hs = kit.install (3, new StubPaxosHost (_, kit))
    val Seq (h1, h2, h3) = hs

    for (h <- hs)
      h.setCohorts((h1, h2, h3))

    import kit.scheduler

    try {

      val k = Bytes (random.nextLong)

      // Propose two values simultaneously, expect one choice.
      val cb1 = h1.paxos.propose (k, 1) .capture()
      val cb2 = h2.paxos.propose (k, 2) .capture()
      kit.messageFlakiness = mf
      kit.runTasks (true, count = 500)
      val v = cb1.passed
      assertResult (v) (cb2.passed)

      // Expect all acceptors closed and in agreement.
      val as = hs map (_.acceptors.get (k))
      assert (as forall (_.isClosed))
      assertResult (1) (as.map (_.getChosen) .flatten.toSet.size)

      Summary (summary.timedout, summary.chosen + v.int)
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
        summary.check (Set (1, 2))
      }

      "stable hosts and a flakey network" taggedAs (Intensive, Periodic) in {
        var summary = Summary (false, Set.empty)
        forAllSeeds { random =>
          summary = checkConsensus (random, 0.1, summary)
        }
        summary.check (Set (1, 2))
      }}}}
