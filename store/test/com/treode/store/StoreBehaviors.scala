package com.treode.store

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.language.postfixOps
import scala.util.Random

import com.treode.concurrent.{Callback, Scheduler}
import com.treode.pickle.Picklers
import org.scalatest.FreeSpec

import Cardinals.{One, Two}
import Fruits.Apple
import TimedTestTools._
import WriteOp._

trait StoreBehaviors {
  this: FreeSpec =>

  def aStore (s: TestableStore) {

    "behave like a Store; when" - {

      "the table is empty" - {

        "reading shoud" - {

          "find 0::None for Apple##1" in {
            val t = nextTable
            s.readAndExpect (1, Get (t, Apple)) (0::None)
          }}

        "writing should" - {

          "allow create Apple::One at ts=0" in {
            val t = nextTable
            val ts = s.writeExpectPass (0, Create (t, Apple, One))
            s.expectCells (t) (Apple##ts::One)
          }

          "allow hold Apple at ts=0" in {
            val t = nextTable
            s.writeExpectPass (0, Hold (t, Apple))
            s.expectCells (t) ()
          }

          "allow update Apple::One at ts=0" in {
            val t = nextTable
            val ts = s.writeExpectPass (0, Update (t, Apple, One))
            s.expectCells (t) (Apple##ts::One)
          }

          "allow delete Apple at ts=0" in {
            val t = nextTable
            val ts = s.writeExpectPass (0, Delete (t, Apple))
            s.expectCells (t) (Apple##ts)
          }}}

      "the table has Apple##ts::One" - {

        def newTableWithData = {
          val t = nextTable
          val ts = s.writeExpectPass (0, Create (t, Apple, One))
          (t, ts)
        }

        "reading should" -  {

          "find ts::One for Apple##ts+1" in {
            val (t, ts) = newTableWithData
            s.readAndExpect (ts+1, Get (t, Apple)) (ts::One)
          }

          "find ts::One for Apple##ts" in {
            val (t, ts) = newTableWithData
            s.readAndExpect (ts, Get (t, Apple)) (ts::One)
          }

          "find 0::None for Apple##ts-1" in {
            val (t, ts) = newTableWithData
            s.readAndExpect (ts-1, Get (t, Apple)) (0::None)
          }}

        "writing should" - {

          "reject create Apple##ts-1" in {
            val (t, ts) = newTableWithData
            s.writeExpectCollisions (ts-1, Create (t, Apple, One)) (0)
          }

          "reject hold Apple##ts-1" in {
            val (t, ts) = newTableWithData
            s.writeExpectAdvance (ts-1, Hold (t, Apple))
          }

          "reject update Apple##ts-1" in {
            val (t, ts) = newTableWithData
            s.writeExpectAdvance (ts-1, Update (t, Apple, One))
          }

          "reject delete Apple##ts-1" in {
            val (t, ts) = newTableWithData
            s.writeExpectAdvance (ts-1, Delete (t, Apple))
          }

          "allow hold Apple at ts+1" in {
            val (t, ts) = newTableWithData
            s.writeExpectPass (ts+1, Hold (t, Apple))
            s.expectCells (t) (Apple##ts::One)
          }

          "allow hold Apple at ts" in {
            val (t, ts) = newTableWithData
            s.writeExpectPass (ts, Hold (t, Apple))
            s.expectCells (t) (Apple##ts::One)
          }

          "allow update Apple::Two at ts+1" taggedAs (com.treode.store.LargeTest) in {
            val (t, ts1) = newTableWithData
            val ts2 = s.writeExpectPass (ts1+1, Update (t, Apple, Two))
            s.expectCells (t) (Apple##ts2::Two, Apple##ts1::One)
          }

          "allow update Apple::Two at ts" in {
            val (t, ts1) = newTableWithData
            val ts2 = s.writeExpectPass (ts1, Update (t, Apple, Two))
            s.expectCells (t) (Apple##ts2::Two, Apple##ts1::One)
          }

          "allow update Apple::One at ts+1" in {
            val (t, ts1) = newTableWithData
            val ts2 = s.writeExpectPass (ts1+1, Update (t, Apple, One))
            s.expectCells (t) (Apple##ts2::One, Apple##ts1::One)
          }

          "allow update Apple::One at ts" in {
            val (t, ts1) = newTableWithData
            val ts2 = s.writeExpectPass (ts1, Update (t, Apple, One))
            s.expectCells (t) (Apple##ts2::One, Apple##ts1::One)
          }

          "allow delete Apple at ts+1" in {
            val (t, ts1) = newTableWithData
            val ts2 = s.writeExpectPass (ts1+1, Delete (t, Apple))
            s.expectCells (t) (Apple##ts2, Apple##ts1::One)
          }

          "allow delete Apple at ts" in {
            val (t, ts1) = newTableWithData
            val ts2 = s.writeExpectPass (ts1, Delete (t, Apple))
            s.expectCells (t) (Apple##ts2, Apple##ts1::One)
          }}}

      "the table has Apple##ts2::Two and Apple##ts1::One" -  {

        var t = TableId (0)
        var ts1 = TxClock.zero
        var ts2 = TxClock.zero

        "setup should pass" in {
          t = nextTable
          ts1 = s.writeExpectPass (0, Create (t, Apple, One))
          ts2 = s.writeExpectPass (ts1, Update (t, Apple, Two))
        }

        "a read should" - {

          "find ts2::Two for Apple##ts2+1" in {
            s.readAndExpect (ts2+1, Get (t, Apple)) (ts2::Two)
          }

          "find ts2::Two for Apple##ts2" in {
            s.readAndExpect (ts2, Get (t, Apple)) (ts2::Two)
          }

          "find ts1::One for Apple##ts2-1" in {
            s.readAndExpect (ts2-1, Get (t, Apple)) (ts1::One)
          }

          "find ts1::One for Apple##ts1+1" in {
            s.readAndExpect (ts1+1, Get (t, Apple)) (ts1::One)
          }

          "find ts1::One for Apple##ts1" in {
            s.readAndExpect (ts1, Get (t, Apple)) (ts1::One)
          }

          "find 0::None for Apple##ts1-1" in {
            s.readAndExpect (ts1-1, Get (t, Apple)) (0::None)
          }}}}}

  def aMultithreadableStore (size: Int, store: TestableStore) {

    "serialize concurrent operations" in {

      object Accounts extends Accessor (1, Picklers.fixedInt, Picklers.fixedInt)

      val threads = 8
      val transfers = 10000
      val opening = 1000

      val createLatch = new CountDownLatch (1)

      val supply = size * opening
      val create =
        for (i <- 0 until size) yield Accounts.create (i, opening)
      store.write (0, create, new StubWriteCallback {
        override def pass (wt: TxClock) = createLatch.countDown()
      })

      createLatch.await (200, TimeUnit.MILLISECONDS)

      val executor = Executors.newScheduledThreadPool (threads)
      val scheduler = Scheduler (executor)
      val brokerLatch = new CountDownLatch (threads)
      val countAuditsPassed = new AtomicInteger (0)
      val countAuditsFailed = new AtomicInteger (0)
      val countTransferPassed = new AtomicInteger (0)
      val countTransferAdvanced = new AtomicInteger (0)

      // Check that the sum of the account balances equals the supply
      def audit (cb: Callback [Unit]): Unit = scheduler.execute {
        val ops = for (i <- 0 until size) yield Accounts.read (i)
        store.read (TxClock.now, ops, new StubReadCallback {
          override def pass (vs: Seq [Value]): Unit = scheduler.execute {
            val total = vs .map (Accounts.value (_) .get) .sum
            if (supply == total)
              countAuditsPassed.incrementAndGet()
            else
              countAuditsFailed.incrementAndGet()
            cb()
          }})
      }

      // Transfer a random amount between two random accounts.
      def transfer (num: Int, cb: Callback [Unit]): Unit = scheduler.execute {
        val x = Random.nextInt (size)
        var y = Random.nextInt (size)
        while (x == y)
          y = Random.nextInt (size)
        val rops = Seq (Accounts.read (x), Accounts.read (y))
        store.read (TxClock.now, rops, new StubReadCallback {
          override def pass (vs: Seq [Value]): Unit = scheduler.execute {
            val ct = vs map (_.time) max
            val Seq (b1, b2) = vs map (Accounts.value (_) .get)
            val n = Random.nextInt (b1)
            val wops = Seq (Accounts.update (x, b1-n), Accounts.update (y, b2+n))
            store.write (ct, wops, new StubWriteCallback {
              override def pass (wt: TxClock): Unit = {
                countTransferPassed.incrementAndGet()
                cb()
              }
              override def advance() = {
                countTransferAdvanced.incrementAndGet()
                cb()
              }
            })
          }})
      }

      // Conduct many transfers.
      def broker (num: Int): Unit = scheduler.execute {
        var i = 0
        val loop = new Callback [Unit] {
          def pass (v: Unit): Unit = {
            if (i < transfers) {
              i += 1
              transfer (num, this)
            } else {
              brokerLatch.countDown()
            }}
          def fail (t: Throwable) = throw t
        }
        transfer (num, loop)
      }

      // Conduct many audits.
      def auditor(): Unit = scheduler.execute {
        val loop = new Callback [Unit] {
          def pass (v: Unit) {
            if (brokerLatch.getCount > 0)
              audit (this)
          }
          def fail (t: Throwable) = throw t
        }
        audit (loop)
      }

      for (i <- 0 until threads)
        broker (i)
      auditor()

      brokerLatch.await (2, TimeUnit.SECONDS)
      executor.shutdown()

      assert (countAuditsPassed.get > 0, "Expected at least one audit to pass.")
      assert (countAuditsFailed.get == 0, "Expected no audits to fail.")
      assert (countTransferPassed.get > 0, "Expected at least one transfer to pass.")
      assert (countTransferAdvanced.get > 0, "Expected at least one trasfer to advance.")
    }}}
