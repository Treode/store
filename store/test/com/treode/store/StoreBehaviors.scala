/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.store

import java.util.concurrent.{CountDownLatch, Executors}
import java.util.concurrent.atomic.AtomicInteger

import com.treode.async._
import com.treode.async.implicits._
import com.treode.async.stubs.{AsyncChecks, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.pickle.Picklers
import com.treode.tags.{Intensive, Periodic}
import org.joda.time.Instant
import org.scalatest.FreeSpec

import Async.{async, guard, latch, when}
import Fruits.Apple
import StoreBehaviors.Accounts
import StoreTestTools._
import WriteOp._

trait StoreBehaviors {
  this: FreeSpec with AsyncChecks =>

  val T1 = TableId (0xA1)

  def testAccountTransfers (ntransfers: Int) (newStore: StoreTestKit => Store) (implicit kit: StoreTestKit) {
    import kit.{scheduler, random}
    import scheduler.whilst

    val naccounts = 100
    val nbrokers = 8
    val opening = 1000
    val supply = naccounts * opening

    implicit val store = newStore (kit)

    val clock = new AtomicInteger (1)
    val countAuditsPassed = new AtomicInteger (0)
    val countTransferPassed = new AtomicInteger (0)
    val countTransferAdvanced = new AtomicInteger (0)

    def nextTick = TxClock (new Instant (clock.getAndIncrement))

    val creates = Seq.tabulate (naccounts) (Accounts.create (_, opening))
    store.write (random.nextTxId, 0, creates: _*) .pass

    var running = true

    // Check that the sum of the account balances equals the supply
    def audit(): Async [Unit] =
      guard [Unit] {
        for {
          cells <- Accounts.recent (nextTick) .toSeq
        } yield {
          val total = cells.map (_.value.get) .sum
          assert (supply == total)
          countAuditsPassed.incrementAndGet()
        }}

    def check(): Async [Unit] =
      guard  {
        val firstTime = Bound (TxClock.MinValue, true)
        val lastTime = Bound (TxClock.MaxValue, true)
        val allTimes = Window.Between (lastTime, firstTime)
        for {
          _history <- Accounts.scan (allTimes, Slice.all) .toSeq
        } yield {
          var tracker = Map.empty [Int, Int] .withDefaultValue (opening)
          val supply = naccounts * opening
          val history = _history
              .groupBy (_.time)
              .toSeq
              .sortBy (_._1)
          for ((time, cells) <- history) {
            for (c <- cells)
              tracker += c.key -> c.value.get
            val total = tracker.values.sum
            assert (supply == total)
          }}}

    // Transfer a random amount between two random accounts.
    def transfer(): Async [Unit] =
      guard [Unit] {
        val x = random.nextInt (naccounts)
        var y = random.nextInt (naccounts)
        while (x == y)
          y = random.nextInt (naccounts)
        val rops = Seq (Accounts.read (x), Accounts.read (y))
        val rt = nextTick
        for {
          vs <- store.read (rt, rops: _*)
          ct = vs.map (_.time) .max
          Seq (b1, b2) = vs map (Accounts.value (_) .get)
          n = random.nextInt (b1)
          wops = Seq (Accounts.update (x, b1-n), Accounts.update (y, b2+n))
          wt <- store.write (random.nextTxId, ct, wops: _*)
        } yield {
          countTransferPassed.incrementAndGet()
        }
      } .recover {
        case _: CollisionException => throw new IllegalArgumentException
        case _: StaleException => countTransferAdvanced.incrementAndGet()
        case _: TimeoutException => ()
      }

    // Conduct many transfers.
    def broker (num: Int): Async [Unit] = {
      var i = 0
      whilst (i < ntransfers) {
        i += 1
        transfer()
      }}

    val brokers = {
      for {
        _ <- (0 until nbrokers) .latch.unit foreach (broker (_))
      } yield {
        running = false
      }}

    // Conduct many audits.
    val auditor = {
      whilst (running) {
        audit()
      }}

    latch (brokers, auditor) .pass
    check() .pass

    assert (countAuditsPassed.get > 0, "Expected at least one audit to pass.")
    assert (countTransferPassed.get > 0, "Expected at least one transfer to pass.")
    assert (countTransferAdvanced.get > 0, "Expected at least one trasfer to advance.")
  }

  def aStore (newStore: StoreTestKit => Store) {

    "behave like a Store; when" - {

      "the table is empty" - {

        "reading shoud" - {

          "find 0::None for Apple##1" in {
            implicit val kit = StoreTestKit.random()
            val s = newStore (kit)
            import kit.scheduler
            s.read (1, Get (T1, Apple)) .expectSeq (0::None)
          }}

        "writing should" - {

          "allow create Apple::1 at ts=0" in {
            implicit val kit = StoreTestKit.random()
            val s = newStore (kit)
            import kit.{random, scheduler}
            val ts = s.write (random.nextTxId, 0, Create (T1, Apple, 1)) .pass
            s.expectCells (T1) (Apple##ts::1)
          }

          "allow hold Apple at ts=0" in {
            implicit val kit = StoreTestKit.random()
            val s = newStore (kit)
            import kit.{random, scheduler}
            s.write (random.nextTxId, 0, Hold (T1, Apple)) .pass
            s.expectCells (T1) ()
          }

          "allow update Apple::1 at ts=0" in {
            implicit val kit = StoreTestKit.random()
            val s = newStore (kit)
            import kit.{random, scheduler}
            val ts = s.write (random.nextTxId, 0, Update (T1, Apple, 1)) .pass
            s.expectCells (T1) (Apple##ts::1)
          }

          "allow delete Apple at ts=0" in {
            implicit val kit = StoreTestKit.random()
            val s = newStore (kit)
            import kit.{random, scheduler}
            val ts = s.write (random.nextTxId, 0, Delete (T1, Apple)) .pass
            s.expectCells (T1) (Apple##ts)
          }}}

      "the table has Apple##ts::1" - {

        def setup() (implicit kit: StoreTestKit) = {
          import kit.{random, scheduler}
          val s = newStore (kit)
          val ts = s.write (random.nextTxId, 0, Create (T1, Apple, 1)) .pass
          (s, ts)
        }

        "reading should" -  {

          "find ts::1 for Apple##ts+1" in {
            implicit val kit = StoreTestKit.random()
            val (s, ts) = setup()
            import kit.{random, scheduler}
            s.read (ts+1, Get (T1, Apple)) .expectSeq (ts::1)
          }

          "find ts::1 for Apple##ts" in {
            implicit val kit = StoreTestKit.random()
            val (s, ts) = setup()
            import kit.{random, scheduler}
            s.read (ts, Get (T1, Apple)) .expectSeq (ts::1)
          }

          "find 0::None for Apple##ts-1" in {
            implicit val kit = StoreTestKit.random()
            val (s, ts) = setup()
            import kit.{random, scheduler}
            s.read (ts-1, Get (T1, Apple)) .expectSeq (0::None)
          }}

        "writing should" - {

          "reject create Apple##ts-1" in {
            implicit val kit = StoreTestKit.random()
            val (s, ts) = setup()
            import kit.{random, scheduler}
            val exn = s.write (random.nextTxId, ts-1, Create (T1, Apple, 1)) .fail [CollisionException]
            assertResult (Seq (0)) (exn.indexes)
          }

          "reject hold Apple##ts-1" in {
            implicit val kit = StoreTestKit.random()
            val (s, ts) = setup()
            import kit.{random, scheduler}
            s.write (random.nextTxId, ts-1, Hold (T1, Apple)) .fail [StaleException]
          }

          "reject update Apple##ts-1" in {
            implicit val kit = StoreTestKit.random()
            val (s, ts) = setup()
            import kit.{random, scheduler}
            s.write (random.nextTxId, ts-1, Update (T1, Apple, 1)) .fail [StaleException]
          }

          "reject delete Apple##ts-1" in {
            implicit val kit = StoreTestKit.random()
            val (s, ts) = setup()
            import kit.{random, scheduler}
            s.write (random.nextTxId, ts-1, Delete (T1, Apple)) .fail [StaleException]
          }

          "allow hold Apple at ts+1" in {
            implicit val kit = StoreTestKit.random()
            val (s, ts) = setup()
            import kit.{random, scheduler}
            s.write (random.nextTxId, ts+1, Hold (T1, Apple)) .pass
            s.expectCells (T1) (Apple##ts::1)
          }

          "allow hold Apple at ts" in {
            implicit val kit = StoreTestKit.random()
            val (s, ts) = setup()
            import kit.{random, scheduler}
            s.write (random.nextTxId, ts, Hold (T1, Apple)) .pass
            s.expectCells (T1) (Apple##ts::1)
          }

          "allow update Apple::2 at ts+1" in {
            implicit val kit = StoreTestKit.random()
            val (s, ts1) = setup()
            import kit.{random, scheduler}
            val ts2 = s.write (random.nextTxId, ts1+1, Update (T1, Apple, 2)) .pass
            s.expectCells (T1) (Apple##ts2::2, Apple##ts1::1)
          }

          "allow update Apple::2 at ts" in {
            implicit val kit = StoreTestKit.random()
            val (s, ts1) = setup()
            import kit.{random, scheduler}
            val ts2 = s.write (random.nextTxId, ts1, Update (T1, Apple, 2)) .pass
            s.expectCells (T1) (Apple##ts2::2, Apple##ts1::1)
          }

          "allow update Apple::1 at ts+1" in {
            implicit val kit = StoreTestKit.random()
            val (s, ts1) = setup()
            import kit.{random, scheduler}
            val ts2 = s.write (random.nextTxId, ts1+1, Update (T1, Apple, 1)) .pass
            s.expectCells (T1) (Apple##ts2::1, Apple##ts1::1)
          }

          "allow update Apple::1 at ts" in {
            implicit val kit = StoreTestKit.random()
            val (s, ts1) = setup()
            import kit.{random, scheduler}
            val ts2 = s.write (random.nextTxId, ts1, Update (T1, Apple, 1)) .pass
            s.expectCells (T1) (Apple##ts2::1, Apple##ts1::1)
          }

          "allow delete Apple at ts+1" in {
            implicit val kit = StoreTestKit.random()
            val (s, ts1) = setup()
            import kit.{random, scheduler}
            val ts2 = s.write (random.nextTxId, ts1+1, Delete (T1, Apple)) .pass
            s.expectCells (T1) (Apple##ts2, Apple##ts1::1)
          }

          "allow delete Apple at ts" in {
            implicit val kit = StoreTestKit.random()
            val (s, ts1) = setup()
            import kit.{random, scheduler}
            val ts2 = s.write (random.nextTxId, ts1, Delete (T1, Apple)) .pass
            s.expectCells (T1) (Apple##ts2, Apple##ts1::1)
          }}}

      "the table has Apple##ts2::2 and Apple##ts1::1" -  {

        def setup() (implicit kit: StoreTestKit) = {
          import kit.{random, scheduler}
          val s = newStore (kit)
          val ts1 = s.write (random.nextTxId, 0, Create (T1, Apple, 1)) .pass
          val ts2 = s.write (random.nextTxId, ts1, Update (T1, Apple, 2)) .pass
          s.expectCells (T1) (Apple##ts2::2, Apple##ts1::1)
          (s, ts1, ts2)
        }

        "a read should" - {

          "find ts2::2 for Apple##ts2+1" in {
            implicit val kit = StoreTestKit.random()
            val (s, _, ts2) = setup()
            import kit.{random, scheduler}
            s.read (ts2+1, Get (T1, Apple)) .expectSeq (ts2::2)
          }

          "find ts2::2 for Apple##ts2" in {
            implicit val kit = StoreTestKit.random()
            val (s, _, ts2) = setup()
            import kit.{random, scheduler}
            s.read (ts2, Get (T1, Apple)) .expectSeq (ts2::2)
          }

          "find ts1::1 for Apple##ts2-1" in {
            implicit val kit = StoreTestKit.random()
            val (s, ts1, ts2) = setup()
            import kit.{random, scheduler}
            s.read (ts2-1, Get (T1, Apple)) .expectSeq (ts1::1)
          }

          "find ts1::1 for Apple##ts1" in {
            implicit val kit = StoreTestKit.random()
            val (s, ts1, _) = setup()
            import kit.{random, scheduler}
            s.read (ts1, Get (T1, Apple)) .expectSeq (ts1::1)
          }

          "find 0::None for Apple##ts1-1" in {
            implicit val kit = StoreTestKit.random()
            val (s, ts1, _) = setup()
            import kit.{random, scheduler}
            s.read (ts1-1, Get (T1, Apple)) .expectSeq (0::None)
          }}}}}}

object StoreBehaviors {

  val Accounts = Accessor (1, Picklers.fixedInt, Picklers.fixedInt)
}
