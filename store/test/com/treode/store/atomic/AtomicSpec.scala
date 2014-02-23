package com.treode.store.atomic

import java.util.concurrent.TimeoutException
import scala.util.Random

import com.treode.async.{AsyncTestTools, CallbackCaptor}
import com.treode.cluster.StubNetwork
import com.treode.store._
import org.scalacheck.Gen
import org.scalatest.{BeforeAndAfterAll, FreeSpec, PropSpec, Specs}
import org.scalatest.prop.PropertyChecks

import AsyncTestTools._
import Cardinals.{One, Two}
import WriteOp._
import TimedTestTools._

class AtomicSpec extends Specs (AtomicBehaviors, AtomicProperties)

object AtomicBehaviors extends FreeSpec with AtomicTestTools with StoreBehaviors {

  private val kit = StubNetwork()
  private val hs = kit.install (3, new StubAtomicHost (_, kit))
  private val host = hs.head
  import kit.{random, scheduler}
  import host.{writeDeputy, write}

  kit.runTasks()

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

    "commit a write" in {
      val ts =
        write (xid, TxClock.zero, Seq (Create (t, k, One)))
            .pass.asInstanceOf [WriteResult.Written] .vt
      val ds = hs map (_.writeDeputy (k))
      hs foreach (_.expectCells (t) (k##ts::One))
    }}

  "The AtomicKit should" - {

    behave like aStore { scheduler =>
      val kit = StubNetwork (new Random (0), scheduler)
      val hs = kit.install (3, new StubAtomicHost (_, kit))
      kit.runTasks()
      new TestableCluster (hs, kit)
    }

    val threaded = {
      val kit = StubNetwork (0, true)
      val hs = kit.install (3, new StubAtomicHost (_, kit))
      new TestableCluster (hs, kit)
    }

    behave like aMultithreadableStore (100, threaded)
  }}

object AtomicProperties extends PropSpec with PropertyChecks with AtomicTestTools {

  val seeds = Gen.choose (0L, Long.MaxValue)

  implicit class RichWriteResult (cb: CallbackCaptor [WriteResult]) {
    import WriteResult._

    def hasWritten: Boolean =
      cb.hasPassed && cb.passed.isInstanceOf [Written]

    def written: TxClock =
      cb.passed.asInstanceOf [Written] .vt

    def hasCollided: Boolean =
      cb.hasPassed && cb.passed.isInstanceOf [Collided]
  }

  def checkConsensus (seed: Long, mf: Double) {
    val kit = StubNetwork (seed)
    val hs = kit.install (3, new StubAtomicHost (_, kit))
    val Seq (h1, h2, h3) = hs
    import kit.{random, scheduler}

    kit.runTasks()

    // Setup.
    val xid1 = TxId (Bytes (random.nextLong))
    val xid2 = TxId (Bytes (random.nextLong))
    val t = TableId (random.nextLong)
    val k = Bytes (random.nextLong)

    // Write two values simultaneously.
    val cb1 = h1.write (xid1, TxClock.zero, Seq (Create (t, k, One))) .capture()
    val cb2 = h2.write (xid2, TxClock.zero, Seq (Create (t, k, Two))) .capture()
    kit.messageFlakiness = mf
    scheduler.runTasks (true)

    // One host might write and the other collide or timeout, or both might timeout.
    if (cb1.hasWritten) {
      assert (cb2.hasCollided || cb2.hasTimedOut)
      val ts = cb1.written
      hs foreach (_.expectCells (t) (k##ts::One))
    } else if (cb2.hasWritten) {
      assert (cb1.hasCollided || cb1.hasTimedOut)
      val ts = cb2.written
      hs foreach (_.expectCells (t) (k##ts::Two))
    } else {
      assert (cb1.hasCollided || cb1.hasTimedOut)
      assert (cb2.hasCollided || cb2.hasTimedOut)
      hs foreach (_.expectCells (t) (k##0))
    }}

  property ("The atomic implemetation should work") {
    forAll (seeds) (checkConsensus (_, 0.0))
  }}
