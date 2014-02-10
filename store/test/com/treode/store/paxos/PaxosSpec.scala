package com.treode.store.paxos

import java.nio.file.Paths
import java.util.concurrent.TimeoutException
import scala.util.Random

import com.treode.async.CallbackCaptor
import com.treode.cluster.StubNetwork
import com.treode.store.{Bytes, Cardinals, LargeTest}
import org.scalacheck.Gen
import org.scalatest.{BeforeAndAfterAll, PropSpec, Specs, WordSpec}
import org.scalatest.prop.PropertyChecks

import Cardinals.{Zero, One, Two}

class PaxosSpec extends Specs (PaxosBehaviors, PaxosProperties)

object PaxosBehaviors extends WordSpec with PaxosTestTools {

  private val kit = StubNetwork()
  private val hs = kit.install (3, new StubPaxosHost (_, kit))
  private val host = hs.head
  import kit.{random, scheduler}
  import host.acceptors
  import host.paxos.lead

  kit.runTasks()

  "The paxos implementation" should {

    val k = Bytes (random.nextLong)

    "yield a value for the leader" in {
      val cb = CallbackCaptor [Bytes]
      lead (k, One, cb)
      kit.runTasks()
      expectResult (One) (cb.passed)
    }

    "leave all acceptors closed and consistent" in {
      val as = hs map (_.acceptors.get (k))
      assert (as forall (_.isClosed))
      expectResult (Set (1)) (as.map (_.getChosen) .flatten.toSet)
    }}}

object PaxosProperties extends PropSpec with PropertyChecks with PaxosTestTools {

  case class Summary (timedout: Boolean, chosen: Set [Int])

  val seeds = Gen.choose (0L, Long.MaxValue)

  def checkConsensus (seed: Long, mf: Double, summary: Summary): Summary = {
    val kit = StubNetwork (seed)
    val hs = kit.install (3, new StubPaxosHost (_, kit))
    val Seq (h1, h2, h3) = hs
    import kit.{random, scheduler}

    kit.runTasks()

    try {

      // Setup.
      val k = Bytes (random.nextLong)
      val cb1 = CallbackCaptor [Bytes]
      val cb2 = CallbackCaptor [Bytes]

      // Proposed two values simultaneously, expect one choice.
      h1.paxos.propose (k, One, cb1)
      h2.paxos.propose (k, Two, cb2)
      kit.messageFlakiness = mf
      scheduler.runTasks (true)
      val v = cb1.passed
      expectResult (v) (cb2.passed)

      // Expect all acceptors closed and in agreement.
      val as = hs map (_.acceptors.get (k))
      assert (as forall (_.isClosed))
      expectResult (1) (as.map (_.getChosen) .flatten.toSet.size)

      Summary (summary.timedout, summary.chosen + v.int)
    } catch {
      case e: TimeoutException =>
        Summary (true, summary.chosen)
    }}

  property ("The acceptors should achieve consensus") {
    var summary = Summary (false, Set.empty)
    forAll (seeds) { seed =>
      summary = checkConsensus (seed, 0.0, summary)
    }
    assert (Seq (1, 2) forall (summary.chosen contains _))
  }

  property ("The acceptors should achieve consensus with a flakey network") {
    var summary = Summary (false, Set.empty)
    forAll (seeds) { seed =>
      summary = checkConsensus (seed, 0.1, summary)
    }
    assert (Seq (1, 2) forall (summary.chosen contains _))
  }}
