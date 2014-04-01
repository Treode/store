package com.treode.store.atomic

import java.util.concurrent.TimeoutException
import scala.util.Random

import com.treode.async.CallbackCaptor
import com.treode.cluster.StubNetwork
import com.treode.store._
import com.treode.tags.{Intensive, Periodic}
import org.scalacheck.Gen
import org.scalatest.{BeforeAndAfterAll, FreeSpec, PropSpec, Suites}
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.prop.PropertyChecks
import org.scalatest.time.SpanSugar

import AtomicTestTools._
import SpanSugar._
import WriteOp._

class AtomicSpec extends Suites (AtomicBehaviors, AtomicProperties)

object AtomicBehaviors extends FreeSpec with StoreBehaviors with TimeLimitedTests {

  val timeLimit = 5 minutes

  private val kit = StubNetwork()
  private val hs = kit.install (3, new StubAtomicHost (_, kit))
  private val Seq (h1, h2, h3) = hs

  for (h <- hs)
    h.setCohorts((h1, h2, h3))

  import kit.{random, scheduler}
  import h1.{write, writer}

  "The transaction implementation should" - {

    val xid = TxId (Bytes (random.nextLong))
    val t = TableId (random.nextLong)
    val k = Bytes (random.nextLong)

    "commit a write" in {
      val ts =
        write (xid, TxClock.zero, Seq (Create (t, k, 1)))
            .pass.asInstanceOf [WriteResult.Written] .vt
      val ds = hs map (_.writer (k))
      hs foreach (_.expectCells (t) (k##ts::1))
    }}

  "The AtomicKit should" - {

    behave like aStore { scheduler =>
      val kit = StubNetwork (new Random (0), scheduler)
      val hs = kit.install (3, new StubAtomicHost (_, kit))
      val Seq (h1, h2, h3) = hs
      for (h <- hs)
        h.setCohorts((h1, h2, h3))
      new TestableCluster (hs, kit)
    }

    val threaded = {
      val kit = StubNetwork (0, true)
      val hs = kit.install (3, new StubAtomicHost (_, kit))
      val Seq (h1, h2, h3) = hs
      for (h <- hs)
        h.setCohorts((h1, h2, h3))
      new TestableCluster (hs, kit)
    }

    behave like aMultithreadableStore (100, threaded)
  }}

object AtomicProperties extends PropSpec with PropertyChecks {

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

    for (h <- hs)
      h.setCohorts((h1, h2, h3))

    import kit.{random, scheduler}

    // Setup.
    val xid1 = TxId (Bytes (random.nextLong))
    val xid2 = TxId (Bytes (random.nextLong))
    val t = TableId (random.nextLong)
    val k = Bytes (random.nextLong)

    // Write two values simultaneously.
    val cb1 = h1.write (xid1, TxClock.zero, Seq (Create (t, k, 1))) .capture()
    val cb2 = h2.write (xid2, TxClock.zero, Seq (Create (t, k, 2))) .capture()
    kit.messageFlakiness = mf
    scheduler.runTasks (true, count = 400)

    // 1 host might write and the other collide or timeout, or both might timeout.
    if (cb1.hasWritten) {
      assert (cb2.hasCollided || cb2.hasTimedOut)
      val ts = cb1.written
      hs foreach (_.expectCells (t) (k##ts::1))
    } else if (cb2.hasWritten) {
      assert (cb1.hasCollided || cb1.hasTimedOut)
      val ts = cb2.written
      hs foreach (_.expectCells (t) (k##ts::2))
    } else {
      assert (cb1.hasCollided || cb1.hasTimedOut)
      assert (cb2.hasCollided || cb2.hasTimedOut)
      hs foreach (_.expectCells (t) (k##0))
    }}

  property ("The atomic implemetation should work", Intensive, Periodic) {
    forAll (seeds) (checkConsensus (_, 0.0))
  }}
