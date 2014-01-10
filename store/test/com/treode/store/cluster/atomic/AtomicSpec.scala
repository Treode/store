package com.treode.store.cluster.atomic

import java.util.concurrent.TimeoutException

import com.treode.async.CallbackCaptor
import com.treode.cluster.StubCluster
import com.treode.store._
import org.scalacheck.Gen
import org.scalatest.{BeforeAndAfterAll, FreeSpec, PropSpec, Specs}
import org.scalatest.prop.PropertyChecks

import Cardinals.{One, Two}
import WriteOp._
import TimedTestTools._

class AtomicSpec extends Specs (AtomicBehaviors, AtomicProperties)

object AtomicBehaviors extends FreeSpec with AtomicTestTools with StoreBehaviors {

  private val kit = StubCluster()
  private val hs = kit.install (3, new StubHost (_, kit))
  private val host = hs.head
  import kit.{random, scheduler}
  import host.{writeDeputy, write}

  "A Deputy should" - {

    val xid = TxId (Bytes (random.nextLong))
    var d: WriteDeputy = null

    "be restoring when first opened" in {
      d = writeDeputy (xid)
      assert (d.isRestoring)
    }}

  "The transaction implementation should" - {

    val xid = TxId (Bytes (random.nextLong))
    val t = TableId (random.nextLong)
    val k = Bytes (random.nextLong)
    val cb = new WriteCaptor

    "commit a write" in {
      write (xid, TxClock.zero, Seq (Create (t, k, One)), cb)
      kit.runTasks()
      cb.passed
    }

    "leave deputies closed and tables consistent" in {
      val ts = cb.passed
      val ds = hs map (_.writeDeputy (k))
      hs foreach (_.mainDb.expectCommitted (xid))
      hs foreach (_.expectCells (t) (k##ts::One))
    }}

  "The AtomicKit should" - {

    behave like aStore (new TestableCluster (hs, kit))

    val threaded = {
      val kit = StubCluster (0, true)
      val hs = kit.install (3, new StubHost (_, kit))
      new TestableCluster (hs, kit)
    }

    behave like aMultithreadableStore (100, threaded)
  }}

object AtomicProperties extends PropSpec with PropertyChecks with AtomicTestTools {

  val seeds = Gen.choose (0L, Long.MaxValue)

  def checkConsensus (seed: Long, mf: Double) {
    val kit = StubCluster (seed)
    val hs = kit.install (3, new StubHost (_, kit))
    val Seq (h1, h2, h3) = hs
    import kit.{random, scheduler}

    // Setup.
    val xid1 = TxId (Bytes (random.nextLong))
    val xid2 = TxId (Bytes (random.nextLong))
    val t = TableId (random.nextLong)
    val k = Bytes (random.nextLong)
    val cb1 = new WriteCaptor
    val cb2 = new WriteCaptor

    // Write two values simultaneously.
    h1.write (xid1, TxClock.zero, Seq (Create (t, k, One)), cb1)
    h2.write (xid2, TxClock.zero, Seq (Create (t, k, Two)), cb2)
    kit.messageFlakiness = mf
    scheduler.runTasks (true)

    // One write might pass and the other collide or timeout, or both might timeout.
    if (cb1.hasPassed) {
      assert (cb2.hasCollided || cb2.hasTimedOut)
      val ts = cb1.passed
      hs foreach (_.mainDb.expectCommitted (xid1))
      hs foreach (_.mainDb.expectAborted (xid2))
      hs foreach (_.expectCells (t) (k##ts::One))
    } else if (cb2.hasPassed) {
      assert (cb1.hasCollided || cb1.hasTimedOut)
      val ts = cb2.passed
      hs foreach (_.mainDb.expectCommitted (xid2))
      hs foreach (_.mainDb.expectAborted (xid1))
      hs foreach (_.expectCells (t) (k##ts::Two))
    } else {
      assert (cb1.hasCollided || cb1.hasTimedOut)
      assert (cb2.hasCollided || cb2.hasTimedOut)
      hs foreach (_.mainDb.expectAborted (xid1))
      hs foreach (_.mainDb.expectAborted (xid2))
      hs foreach (_.expectCells (t) (k##0))
    }}

  property ("The atomic implemetation should work") {
    forAll (seeds) (checkConsensus (_, 0.0))
  }}
