package com.treode.store.cluster.atomic

import java.util.concurrent.TimeoutException

import com.treode.concurrent.CallbackCaptor
import com.treode.store._
import org.scalacheck.Gen
import org.scalatest.{BeforeAndAfterAll, PropSpec, Specs, WordSpec}
import org.scalatest.prop.PropertyChecks

import WriteOp._
import TimedTestTools._

class AtomicSpec extends Specs (AtomicBehaviors, AtomicProperties)

object AtomicBehaviors extends WordSpec with BeforeAndAfterAll with AtomicTestTools {

  private val kit = new StubCluster (0, 3)
  private val hs @ Seq (_, _, host) = kit.hosts
  import kit.{random, scheduler}
  import host.{deputy, write}

  override def afterAll() {
    kit.cleanup()
  }

  "A Deputy" should {

    val xid = TxId (Bytes (random.nextLong))
    var d: Deputy = null

    "be restoring when first opened" in {
      d = deputy (xid)
      assert (d.isRestoring)
    }}

  "The atomic implementation" should {

    val xid = TxId (Bytes (random.nextLong))
    val t = TableId (random.nextLong)
    val k = Bytes (random.nextLong)
    val cb = new WriteCaptor

    "commit a write" in {
      write (WriteBatch (xid, TxClock.zero, TxClock.zero, Create (t, k, One)), cb)
      kit.runTasks()
      cb.passed
    }

    "leave deputies closed and tables consistent" in {
      val ts = cb.passed
      val ds = hs map (_.atomic.Deputies.get (k))
      hs foreach (_.mainDb.expectCommitted (xid))
      hs foreach (_.expectCells (t) (k##ts::1))
    }}}

object AtomicProperties extends PropSpec with PropertyChecks with AtomicTestTools {

  val seeds = Gen.choose (0L, Long.MaxValue)

  def checkConsensus (seed: Long, mf: Double) {
    val kit = new StubCluster (seed, 3)
    val Seq (h1, h2, h3) = kit.hosts
    val hs = kit.hosts
    import kit.{random, scheduler}

    // Setup.
    val xid1 = TxId (Bytes (random.nextLong))
    val xid2 = TxId (Bytes (random.nextLong))
    val t = TableId (random.nextLong)
    val k = Bytes (random.nextLong)
    val cb1 = new WriteCaptor
    val cb2 = new WriteCaptor

    // Write two values simultaneously.
    h1.write (WriteBatch (xid1, TxClock.zero, TxClock.zero, Create (t, k, One)), cb1)
    h2.write (WriteBatch (xid2, TxClock.zero, TxClock.zero, Create (t, k, Two)), cb2)
    kit.messageFlakiness = mf
    scheduler.runTasks (true)

    // One write might pass and the other collide or timeout, or both might timeout.
    if (cb1.hasPassed) {
      assert (cb2.hasCollided || cb2.hasTimedOut)
      val ts = cb1.passed
      hs foreach (_.mainDb.expectCommitted (xid1))
      hs foreach (_.mainDb.expectAborted (xid2))
      hs foreach (_.expectCells (t) (k##ts::1))
    } else if (cb2.hasPassed) {
      assert (cb1.hasCollided || cb1.hasTimedOut)
      val ts = cb2.passed
      hs foreach (_.mainDb.expectCommitted (xid2))
      hs foreach (_.mainDb.expectAborted (xid1))
      hs foreach (_.expectCells (t) (k##ts::2))
    } else {
      assert (cb1.hasCollided || cb1.hasTimedOut)
      assert (cb2.hasCollided || cb2.hasTimedOut)
      hs foreach (_.mainDb.expectAborted (xid1))
      hs foreach (_.mainDb.expectAborted (xid2))
      hs foreach (_.expectCells (t) (k##0))
    }

    // Cleanup.
    kit.messageFlakiness = 0.0
    kit.cleanup()
  }

  property ("The atomic implemetation should work", LargeTest) {
    forAll (seeds) (checkConsensus (_, 0.0))
  }}
