package com.treode.store.atomic

import java.util.concurrent.TimeoutException
import scala.util.Random

import com.treode.async.stubs.{AsyncChecks, CallbackCaptor}
import com.treode.cluster.stubs.StubNetwork
import com.treode.store._
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.{BeforeAndAfterAll, FreeSpec, PropSpec, Suites}
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.SpanSugar

import AtomicTestTools._
import SpanSugar._
import WriteOp._

class AtomicSpec extends FreeSpec with StoreBehaviors with AsyncChecks {

  def check (random: Random, mf: Double) {

    val kit = StubNetwork (random)
    val hs = kit.install (3, new StubAtomicHost (_, kit))
    val Seq (h1, h2, h3) = hs

    for (h <- hs)
      h.setAtlas (settled (h1, h2, h3))

    import kit.scheduler

    // Setup.
    val xid1 = TxId (random.nextLong, 0)
    val xid2 = TxId (random.nextLong, 0)
    val t = TableId (random.nextLong)
    val k = Bytes (random.nextLong)

    // Write two values simultaneously.
    val cb1 = h1.write (xid1, TxClock.zero, Create (t, k, 1)) .capture()
    val cb2 = h2.write (xid2, TxClock.zero, Create (t, k, 2)) .capture()
    kit.messageFlakiness = mf
    scheduler.runTasks (true, count = 400)

    // 1 host might write and the other collide or timeout, or both might timeout.
    if (cb1.hasPassed) {
      assert (cb2.hasFailed [CollisionException] || cb2.hasFailed [TimeoutException])
      val ts = cb1.passed
      hs foreach (_.expectCells (t) (k##ts::1))
    } else if (cb2.hasPassed) {
      assert (cb1.hasFailed [CollisionException] || cb1.hasFailed [TimeoutException])
      val ts = cb2.passed
      hs foreach (_.expectCells (t) (k##ts::2))
    } else {
      assert (cb1.hasFailed [CollisionException] || cb1.hasFailed [TimeoutException])
      assert (cb2.hasFailed [CollisionException] || cb2.hasFailed [TimeoutException])
      hs foreach (_.expectCells (t) (k##0))
    }}

  "The atomic implementation should" - {

    behave like aStore { scheduler =>
      val kit = StubNetwork (new Random (0), scheduler)
      val hs = kit.install (3, new StubAtomicHost (_, kit))
      val Seq (h1, h2, h3) = hs
      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))
      new TestableCluster (hs, kit)
    }

    behave like aMultithreadableStore (100) {
      val kit = StubNetwork (Random, true)
      val hs = kit.install (3, new StubAtomicHost (_, kit))
      val Seq (h1, h2, h3) = hs
      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))
      new TestableCluster (hs, kit)
    }

    "achieve consensus with" - {

      "stable hosts and a reliable network" taggedAs (Intensive, Periodic) in {
        forAllSeeds (check (_, 0.0))
      }}

    "rebalance" in {
      val kit = StubNetwork()
      import kit.scheduler

      val hs = kit.install (4, new StubAtomicHost (_, kit))
      val Seq (h1, h2, h3, h4) = hs
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId, null)
      h1.issueAtlas (settled (h1, h2, h3))
      h1.issueAtlas (moving (h1, h2, h3) (h1, h2, h4))

      val xid = TxId (0x6196E3A0F6804B8FL, 0)
      val t = TableId (0xA49381B59A722319L)
      val k = Bytes (0xB3334572873016E4L)
      val ts = h1.write (xid, TxClock.zero, Create (t, k, 1)) .pass

      for (h <- hs)
        h.expectCells (t) (k##ts::1)
      kit.runTasks (count = 1000, timers = true)
      expectAtlas (3, settled (h1, h2, h4)) (hs)
      for (h <- Seq (h1, h2, h4))
        h.expectCells (t) (k##ts::1)
      h3.expectCells (t) ()
    }}}
