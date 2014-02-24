package com.treode.store

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.language.postfixOps
import scala.util.Random

import com.treode.async._
import com.treode.pickle.Picklers
import org.scalatest.FreeSpec

import Async.{async, whilst}
import AsyncConversions._
import AsyncTestTools._
import Cardinals.{One, Two}
import Fruits.Apple
import TimedTestTools._
import WriteOp._

trait StoreBehaviors {
  this: FreeSpec =>

  def aStore (newStore: StubScheduler => TestableStore) {

    "behave like a Store; when" - {

      "the table is empty" - {

        def setup() (implicit scheduler: StubScheduler) = {
          val s = newStore (scheduler)
          val t = nextTable
          (s, t)
        }

        "reading shoud" - {

          "find 0::None for Apple##1" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t) = setup()
            s.read (1, Get (t, Apple)) .expectSeq (0::None)
          }}

        "writing should" - {

          "allow create Apple::One at ts=0" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t) = setup()
            val ts = s.write (0, Create (t, Apple, One)) .expectWritten
            s.expectCells (t) (Apple##ts::One)
          }

          "allow hold Apple at ts=0" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t) = setup()
            s.write (0, Hold (t, Apple)) .expectWritten
            s.expectCells (t) ()
          }

          "allow update Apple::One at ts=0" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t) = setup()
            val ts = s.write (0, Update (t, Apple, One)) .expectWritten
            s.expectCells (t) (Apple##ts::One)
          }

          "allow delete Apple at ts=0" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t) = setup()
            val ts = s.write (0, Delete (t, Apple)) .expectWritten
            s.expectCells (t) (Apple##ts)
          }}}

      "the table has Apple##ts::One" - {

        def setup() (implicit scheduler: StubScheduler) = {
          val s = newStore (scheduler)
          val t = nextTable
          val ts = s.write (0, Create (t, Apple, One)) .expectWritten
          (s, t, ts)
        }

        "reading should" -  {

          "find ts::One for Apple##ts+1" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts) = setup()
            s.read (ts+1, Get (t, Apple)) .expectSeq (ts::One)
          }

          "find ts::One for Apple##ts" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts) = setup()
            s.read (ts, Get (t, Apple)) .expectSeq (ts::One)
          }

          "find 0::None for Apple##ts-1" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts) = setup()
            s.read (ts-1, Get (t, Apple)) .expectSeq (0::None)
          }}

        "writing should" - {

          "reject create Apple##ts-1" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts) = setup()
            s.write (ts-1, Create (t, Apple, One)) .expectCollided (0)
          }

          "reject hold Apple##ts-1" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts) = setup()
            s.write (ts-1, Hold (t, Apple)) .expectStale
          }

          "reject update Apple##ts-1" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts) = setup()
            s.write (ts-1, Update (t, Apple, One)) .expectStale
          }

          "reject delete Apple##ts-1" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts) = setup()
            s.write (ts-1, Delete (t, Apple)) .expectStale
          }

          "allow hold Apple at ts+1" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts) = setup()
            s.write (ts+1, Hold (t, Apple)) .expectWritten
            s.expectCells (t) (Apple##ts::One)
          }

          "allow hold Apple at ts" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts) = setup()
            s.write (ts, Hold (t, Apple)) .expectWritten
            s.expectCells (t) (Apple##ts::One)
          }

          "allow update Apple::Two at ts+1" taggedAs (com.treode.store.LargeTest) in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts1) = setup()
            val ts2 = s.write (ts1+1, Update (t, Apple, Two)) .expectWritten
            s.expectCells (t) (Apple##ts2::Two, Apple##ts1::One)
          }

          "allow update Apple::Two at ts" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts1) = setup()
            val ts2 = s.write (ts1, Update (t, Apple, Two)) .expectWritten
            s.expectCells (t) (Apple##ts2::Two, Apple##ts1::One)
          }

          "allow update Apple::One at ts+1" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts1) = setup()
            val ts2 = s.write (ts1+1, Update (t, Apple, One)) .expectWritten
            s.expectCells (t) (Apple##ts2::One, Apple##ts1::One)
          }

          "allow update Apple::One at ts" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts1) = setup()
            val ts2 = s.write (ts1, Update (t, Apple, One)) .expectWritten
            s.expectCells (t) (Apple##ts2::One, Apple##ts1::One)
          }

          "allow delete Apple at ts+1" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts1) = setup()
            val ts2 = s.write (ts1+1, Delete (t, Apple)) .expectWritten
            s.expectCells (t) (Apple##ts2, Apple##ts1::One)
          }

          "allow delete Apple at ts" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts1) = setup()
            val ts2 = s.write (ts1, Delete (t, Apple)) .expectWritten
            s.expectCells (t) (Apple##ts2, Apple##ts1::One)
          }}}

      "the table has Apple##ts2::Two and Apple##ts1::One" -  {

        def setup() (implicit scheduler: StubScheduler) = {
          val s = newStore (scheduler)
          val t = nextTable
          val ts1 = s.write (0, Create (t, Apple, One)) .expectWritten
          val ts2 = s.write (ts1, Update (t, Apple, Two)) .expectWritten
          s.expectCells (t) (Apple##ts2::Two, Apple##ts1::One)
          (s, t, ts1, ts2)
        }

        "a read should" - {

          "find ts2::Two for Apple##ts2+1" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts1, ts2) = setup()
            s.read (ts2+1, Get (t, Apple)) .expectSeq (ts2::Two)
          }

          "find ts2::Two for Apple##ts2" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts1, ts2) = setup()
            s.read (ts2, Get (t, Apple)) .expectSeq (ts2::Two)
          }

          "find ts1::One for Apple##ts2-1" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts1, ts2) = setup()
            s.read (ts2-1, Get (t, Apple)) .expectSeq (ts1::One)
          }

          "find ts1::One for Apple##ts1+1" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts1, ts2) = setup()
            s.read (ts1+1, Get (t, Apple)) .expectSeq (ts1::One)
          }

          "find ts1::One for Apple##ts1" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts1, ts2) = setup()
            s.read (ts1, Get (t, Apple)) .expectSeq (ts1::One)
          }

          "find 0::None for Apple##ts1-1" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts1, ts2) = setup()
            s.read (ts1-1, Get (t, Apple)) .expectSeq (0::None)
          }}}}}

  def aMultithreadableStore (size: Int, store: TestableStore) {

    "serialize concurrent operations" in {

      object Accounts extends Accessor (1, Picklers.fixedInt, Picklers.fixedInt)

      val threads = 8
      val transfers = 100
      val opening = 1000

      val executor = Executors.newScheduledThreadPool (threads)
      implicit val scheduler = Scheduler (executor)

      val supply = size * opening
      val create =
        for (i <- 0 until size) yield Accounts.create (i, opening)
      store.write (0, create: _*) .await()

      val brokerLatch = new CountDownLatch (threads)
      val countAuditsPassed = new AtomicInteger (0)
      val countAuditsFailed = new AtomicInteger (0)
      val countTransferPassed = new AtomicInteger (0)
      val countTransferAdvanced = new AtomicInteger (0)

      var running = true

      // Check that the sum of the account balances equals the supply
      def audit(): Async [Unit] = {
        val ops = for (i <- 0 until size) yield Accounts.read (i)
        for {
          accounts <- store.read (TxClock.now, ops: _*)
          total = accounts.map (Accounts.value (_) .get) .sum
        } yield {
          if (supply == total)
            countAuditsPassed.incrementAndGet()
          else
            countAuditsFailed.incrementAndGet()
        }}

      // Transfer a random amount between two random accounts.
      def transfer(): Async [Unit] = {
        val x = Random.nextInt (size)
        var y = Random.nextInt (size)
        while (x == y)
          y = Random.nextInt (size)
        val rops = Seq (Accounts.read (x), Accounts.read (y))
        for {
          vs <- store.read (TxClock.now, rops: _*)
          ct = vs.map (_.time) .max
          Seq (b1, b2) = vs map (Accounts.value (_) .get)
          n = Random.nextInt (b1)
          wops = Seq (Accounts.update (x, b1-n), Accounts.update (y, b2+n))
          result <- store.write (ct, wops: _*)
        } yield {
          import WriteResult._
          result match {
            case Written (_) => countTransferPassed.incrementAndGet()
            case Collided (_) => throw new IllegalArgumentException
            case Stale => countTransferAdvanced.incrementAndGet()
          }}}

      // Conduct many transfers.
      def broker (num: Int): Async [Unit] = {
        var i = 0
        whilst (i < transfers) {
          i += 1
          transfer()
        }}

      val brokers = {
        for {
          _ <- (0 until threads) .latch.unit (broker (_))
        } yield {
          running = false
        }}

      def sleep (millis: Int): Async [Unit] =
        async (cb => scheduler.delay (millis) (cb.pass()))

      // Conduct many audits.
      val auditor = {
        whilst (running) {
          audit() .flatMap (_ => sleep (100))
        }}

      Latch.pair (brokers, auditor) .await()
      executor.shutdown()

      assert (countAuditsPassed.get > 0, "Expected at least one audit to pass.")
      assert (countAuditsFailed.get == 0, "Expected no audits to fail.")
      assert (countTransferPassed.get > 0, "Expected at least one transfer to pass.")
      assert (countTransferAdvanced.get > 0, "Expected at least one trasfer to advance.")
    }}}
