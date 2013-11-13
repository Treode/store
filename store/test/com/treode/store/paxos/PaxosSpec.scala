package com.treode.store.paxos

import java.nio.file.Paths
import java.util.concurrent.TimeoutException
import scala.util.Random

import com.treode.cluster.{ClusterStubBase, HostId}
import com.treode.concurrent.{Callback, StubScheduler}
import com.treode.store.{Bytes, LargeTest, SimpleAccessor, TestFiles}
import com.treode.store.local.temp.TempSimpleStore
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, PropSpec, Specs, WordSpec}
import org.scalatest.prop.PropertyChecks

class PaxosSpec extends Specs (PaxosBehaviors, PaxosProperties)

trait PaxosSpecTools {

  class Captor [T] extends Callback [T] {

    private var _v: T = null.asInstanceOf [T]

    def v: T = {
      assert (_v != null, "Callback was not invoked")
      _v
    }

    def pass (v: T) = _v = v

    def fail (t: Throwable) = throw t
  }

  def capture [T] (f: Callback [T] => Any) (implicit scheduler: StubScheduler): T = {
    val cb = new Captor [T]
    f (cb)
    scheduler.runTasks()
    cb.v
  }

  private [paxos] class ClusterStub (seed: Long, nhosts: Int) extends ClusterStubBase (seed, nhosts) {

    class HostStub (id: HostId) extends HostStubBase (id) {

      val store = new TempSimpleStore

      val paxos = new PaxosKit () (HostStub.this, store)

      def db = paxos.Acceptors.db

      override def cleanup() {
        paxos.close()
      }}

    def newHost (id: HostId) = new HostStub (id)
  }

  implicit class TestableAcceptor (a: Acceptor) {

    def isRestoring = a.state == a.Restoring
    def isDeliberating = classOf [Acceptor#Deliberating] .isInstance (a.state)
    def isClosed = classOf [Acceptor#Closed] .isInstance (a.state)

    def getChosen: Option [Int] = {
      if (isClosed)
        Some (a.state.asInstanceOf [Acceptor#Closed] .chosen.int)
      else if (isDeliberating)
        a.state.asInstanceOf [Acceptor#Deliberating] .proposal.map (_._2.int)
      else
        None
    }}

  implicit class TestableSimpleAccessor [K, V] (db: SimpleAccessor [K, V]) {

    def getAndCapture (k: K) (implicit scheduler: StubScheduler): Option [V] =
      capture [Option [V]] (db.get (k, _))
  }}

object PaxosBehaviors extends WordSpec with BeforeAndAfterAll with MockFactory with PaxosSpecTools {

  val failed = afterWord ("failed")
  val sent = afterWord ("sent")

  val kit = new ClusterStub (0, 3)
  val hs @ Seq (_, _, host) = kit.hosts
  import kit.{random, scheduler}
  import host.paxos.{Acceptors, lead}

  override def afterAll() {
    kit.cleanup()
  }

  "An acceptor" should {

    val k = Bytes (random.nextLong)
    var a: Acceptor = null

    "be restoring when first opened" in {
      a = Acceptors.get (k)
      assert (a.isRestoring)
    }

    "be deliberating after running tasks" in {
      a.query (host.peers.get (host.localId), 0, Bytes (0))
      kit.runTasks()
      assert (a.isDeliberating)
    }

    "be restoring when removed and reopened" in {
      Acceptors.remove (k, a)
      a = Acceptors.get (k)
      assert (a.isRestoring)
    }}

  "The paxos implementation" should {

    val k = Bytes (random.nextLong)

    "yield a value for the leader" in {
      val cb = mock [Callback [Bytes]]
      lead (k, Bytes (1), cb)
      (cb.apply _) .expects (Bytes (1)) .once()
      kit.runTasks()
    }

    "leave all acceptors closed and consistent" in {
      val as = hs map (_.paxos.Acceptors.get (k))
      assert (as forall (_.isClosed))
      expectResult (Set (1)) (as.map (_.getChosen) .flatten.toSet)
    }}}

object PaxosProperties extends PropSpec with PropertyChecks with PaxosSpecTools {

  case class Summary (timedout: Boolean, chosen: Set [Int])

  val seeds = Gen.choose (0L, Long.MaxValue)

  def checkConsensus (seed: Long, mf: Double, summary: Summary): Summary = {
    val kit = new ClusterStub (seed, 3)
    val Seq (h1, h2, h3) = kit.hosts
    var hs = kit.hosts
    import kit.{random, scheduler}

    def cleanup() {
      try {
        kit.messageFlakiness = mf
        kit.runTasks()
        kit.cleanup()
      } catch {
        case e: TimeoutException => ()
      }}

    try {

      // Setup.
      val k = Bytes (random.nextLong)
      val cb1 = new Captor [Bytes]
      val cb2 = new Captor [Bytes]

      // Proposed two values simultaneously, expect one choice.
      h1.paxos.propose (k, Bytes (1), cb1)
      h2.paxos.propose (k, Bytes (2), cb2)
      kit.messageFlakiness = mf
      scheduler.runTasks (true)
      val v = cb1.v
      expectResult (v) (cb2.v)

      // Expect all acceptors closed and in agreement.
      val as = hs.map (_.db.getAndCapture (k) (scheduler)) .toSet
      assert (as.size == 1)
      expectResult (Some (PaxosStatus.Closed (v))) (as.head)

      // Cleanup.
      kit.cleanup()

      Summary (summary.timedout, summary.chosen + v.int)
    } catch {
      case e: TimeoutException =>
        cleanup()
        Summary (true, summary.chosen)
      case e: Throwable =>
        cleanup()
        e.printStackTrace; throw e
    }}

  property ("The acceptors should achieve consensus", LargeTest) {
    var summary = Summary (false, Set.empty)
    forAll (seeds) { seed =>
      summary = checkConsensus (seed, 0.0, summary)
    }
    assert (summary.chosen contains 1)
  }

  property ("The acceptors should achieve consensus with a flakey network", LargeTest) {
    var summary = Summary (false, Set.empty)
    forAll (seeds) { seed =>
      summary = checkConsensus (seed, 0.1, summary)
    }
    assert (summary.chosen contains 1)
  }}
