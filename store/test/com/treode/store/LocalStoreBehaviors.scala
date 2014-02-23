package com.treode.store

import com.treode.async.{Async, AsyncTestTools, Callback, StubScheduler}
import com.treode.store.locks.LockSet
import org.scalatest.FreeSpec

import Async.{async, guard, supply}
import AsyncTestTools._
import Cardinals.{One, Two}
import Fruits.Apple
import TimedTestTools._
import WriteOp._

trait LocalStoreBehaviors extends StoreBehaviors {
  this: FreeSpec =>

  class Adaptor (s: TestableLocalStore) extends TestableStore {

    def read (rt: TxClock, ops: ReadOp*): Async [Seq [Value]] =
      s.read (rt, ops)

    private def commit (wt: TxClock, ops: Seq [WriteOp], locks: LockSet): Async [WriteResult] =
      guard {
        for {
          _ <- s.commit (wt, ops)
        } yield {
          locks.release()
          WriteResult.Written (wt)
        }}

    def write (ct: TxClock, ops: WriteOp*): Async [WriteResult] =
      guard {
        for {
          prepare <- s.prepare (ct, ops)
          write <- prepare match {
            case PrepareResult.Prepared (vt, locks) =>
              commit (vt + 7, ops, locks) // Add gaps to the history.
            case PrepareResult.Collided (ks) =>
              supply (WriteResult.Collided (ks))
            case PrepareResult.Stale =>
              supply (WriteResult.Stale)
          }
        } yield write
      }

    def expectCells (t: TableId) (expected: TimedCell*): Unit =
      s.expectCells (t) (expected: _*)

    def runTasks() = ()
  }

  def aLocalStore (newStore: StubScheduler => TestableLocalStore) {

    aStore (scheduler => new Adaptor (newStore (scheduler)))

    "behave like a LocalStore; when" - {

      "the table is empty" - {

        def setup() (implicit scheduler: StubScheduler) = {
          val s = newStore (scheduler)
          val t = nextTable
          (s, t)
        }

        "and a write aborts should" - {

          "ignore create Apple::1 at ts=0" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t) = setup()
            s.prepare (0, Seq (Create (t, Apple, One))) .abort()
            s.expectCells (t) ()
          }

          "ignore hold Apple at ts=0" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t) = setup()
            s.prepare (0, Seq (Hold (t, Apple))) .abort()
            s.expectCells (t) ()
          }

          "ignore update Apple::1 at ts=0" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t) = setup()
            s.prepare (0, Seq (Update (t, Apple, One))) .abort()
            s.expectCells (t) ()
          }

          "ignore delete Apple at ts=0" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t) = setup()
            s.prepare (0, Seq (Delete (t, Apple))) .abort()
            s.expectCells (t) ()
          }}}

      "when the table has Apple##ts::1" - {

        def setup() (implicit scheduler: StubScheduler) = {
          val s = newStore (scheduler)
          val t = nextTable
          val ops = Seq (Create (t, Apple, One))
          val (vt, locks) = s.prepare (0, ops) .expectPrepared
          val ts = vt + 7
          s.commit (ts, ops) .pass
          locks.release()
          (s, t, ts)
        }

        "and a write aborts should" -  {

          "ignore update Apple::2 at ts+1" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts1) = setup()
            s.prepare (ts1+1, Seq (Update (t, Apple, Two))) .abort()
            s.expectCells (t) (Apple##ts1::One)
          }

          "ignore delete Apple at ts+1" in {
            implicit val scheduler = StubScheduler.random()
            val (s, t, ts1) = setup()
            s.prepare (ts1+1, Seq (Delete (t, Apple))) .abort()
            s.expectCells (t) (Apple##ts1::One)
          }}}}}}
