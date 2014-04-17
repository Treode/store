package com.treode.store.atomic

import java.util.concurrent.TimeoutException
import scala.util.Random

import com.treode.async.{AsyncChecks, CallbackCaptor}
import com.treode.cluster.StubNetwork
import com.treode.store._
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.{BeforeAndAfterAll, FreeSpec, PropSpec, Suites}
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.SpanSugar

import AtomicTestTools._
import SpanSugar._
import WriteOp._

class AtomicSpec extends FreeSpec with StoreBehaviors with AsyncChecks {

  implicit class RichWriteResult (cb: CallbackCaptor [WriteResult]) {
    import WriteResult._

    def writeHasPassed: Boolean =
      cb.hasPassed && cb.passed.isInstanceOf [Written]

    def written: TxClock =
      cb.passed.asInstanceOf [Written] .vt

    def writeHasCollided: Boolean =
      cb.hasPassed && cb.passed.isInstanceOf [Collided]

    def writeHasTimedOut: Boolean =
      cb.hasPassed && cb.passed == Timeout
  }

  def check (random: Random, mf: Double) {

    val kit = StubNetwork (random)
    val hs = kit.install (3, new StubAtomicHost (_, kit))
    val Seq (h1, h2, h3) = hs

    for (h <- hs)
      h.setCohorts((h1, h2, h3))

    import kit.scheduler

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
    if (cb1.writeHasPassed) {
      assert (cb2.writeHasCollided || cb2.writeHasTimedOut)
      val ts = cb1.written
      hs foreach (_.expectCells (t) (k##ts::1))
    } else if (cb2.writeHasPassed) {
      assert (cb1.writeHasCollided || cb1.writeHasTimedOut)
      val ts = cb2.written
      hs foreach (_.expectCells (t) (k##ts::2))
    } else {
      assert (cb1.writeHasCollided || cb1.writeHasTimedOut)
      assert (cb2.writeHasCollided || cb2.writeHasTimedOut)
      hs foreach (_.expectCells (t) (k##0))
    }}

  "The atomic implementation should" - {

    behave like aStore { scheduler =>
      val kit = StubNetwork (new Random (0), scheduler)
      val hs = kit.install (3, new StubAtomicHost (_, kit))
      val Seq (h1, h2, h3) = hs
      for (h <- hs)
        h.setCohorts((h1, h2, h3))
      new TestableCluster (hs, kit)
    }

    behave like aMultithreadableStore (100) {
      val kit = StubNetwork (Random, true)
      val hs = kit.install (3, new StubAtomicHost (_, kit))
      val Seq (h1, h2, h3) = hs
      for (h <- hs)
        h.setCohorts((h1, h2, h3))
      new TestableCluster (hs, kit)
    }

    "achieve consensus with" - {

      "stable hosts and a reliable network" taggedAs (Intensive, Periodic) in {
        forAllSeeds (check (_, 0.0))
      }}}}
