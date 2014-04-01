package com.treode.store.paxos

import java.nio.file.Paths
import java.util.concurrent.TimeoutException
import scala.util.Random

import com.treode.async.AsyncChecks
import com.treode.cluster.StubNetwork
import com.treode.store.Bytes
import com.treode.tags.{Intensive, Periodic}
import org.scalacheck.Gen
import org.scalatest.{PropSpec, Suites, WordSpec}

import PaxosTestTools._

class PaxosSpec extends Suites (PaxosBehaviors, new PaxosProperties)

object PaxosBehaviors extends WordSpec {

  private val kit = StubNetwork()
  private val hs = kit.install (3, new StubPaxosHost (_, kit))
  private val Seq (h1, h2, h3) = hs

  for (h <- hs)
    h.setCohorts((h1, h2, h3))

  import kit.{random, scheduler}
  import h1.acceptors
  import h1.paxos.lead

  kit.runTasks()

  "The paxos implementation" should {

    val k = Bytes (random.nextLong)

    "yield a value for the leader" in {
      lead (k, 1) .expect (1)
    }

    "leave all acceptors closed and consistent" in {
      val as = hs map (_.acceptors.get (k))
      assert (as forall (_.isClosed))
      assertResult (Set (1)) (as.map (_.getChosen) .flatten.toSet)
    }}}

class PaxosProperties extends PropSpec with AsyncChecks {

  case class Summary (timedout: Boolean, chosen: Set [Int])

  def checkConsensus (random: Random, mf: Double, summary: Summary): Summary = {
    val kit = StubNetwork (random)
    val hs = kit.install (3, new StubPaxosHost (_, kit))
    val Seq (h1, h2, h3) = hs

    for (h <- hs)
    h.setCohorts((h1, h2, h3))

    import kit.scheduler

    try {

      val k = Bytes (random.nextLong)

      // Proposed two values simultaneously, expect one choice.
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

  property ("The acceptors should achieve consensus", Intensive, Periodic) {
    var summary = Summary (false, Set.empty)
    forAllSeeds { random =>
      summary = checkConsensus (random, 0.0, summary)
    }
    if (intensity == "standard")
      assert (Seq (1, 2) forall (summary.chosen contains _))
  }

  property ("The acceptors should achieve consensus with a flakey network", Intensive, Periodic) {
    var summary = Summary (false, Set.empty)
    forAllSeeds { random =>
      summary = checkConsensus (random, 0.1, summary)
    }
    if (intensity == "standard")
      assert (Seq (1, 2) forall (summary.chosen contains _))
  }}
