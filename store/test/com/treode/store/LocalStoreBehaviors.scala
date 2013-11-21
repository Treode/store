package com.treode.store

import com.treode.async.Callback
import com.treode.store.local.temp.TestableTempKit
import org.scalatest.FreeSpec
import scala.util.Random

import Cardinals.{One, Two}
import Fruits.Apple
import TimedTestTools._
import WriteOp._

trait LocalStoreBehaviors extends StoreBehaviors {
  this: FreeSpec =>

  class Adaptor (s: TestableLocalStore) extends TestableStore {

    def read (rt: TxClock, ops: Seq [ReadOp], cb: ReadCallback): Unit =
      s.read (rt, ops, cb)

    def write (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback) {
      s.prepare (ct, ops, new PrepareCallback {
        def pass (tx: Transaction) {
          val wt = tx.ft + 7 // Add gaps to the history.
          tx.commit (wt, new Callback [Unit] {
            def pass (v: Unit) = cb (wt)
            def fail (t: Throwable) = cb.fail (t)
          })
        }
        def fail (t: Throwable) = cb.fail (t)
        def collisions (ks: Set [Int]) = cb.collisions (ks)
        def advance() = cb.advance()
      })
    }

    def expectCells (t: TableId) (expected: TimedCell*): Unit =
      s.expectCells (t) (expected: _*)

    def runTasks() = ()
  }

  def aLocalStore (s: TestableLocalStore) {

    aStore (new Adaptor (s))

    "behave like a LocalStore; when" - {

      "the table is empty" - {

        "and a write aborts should" - {

          "ignore create Apple::1 at ts=0" in {
            val t = nextTable
            s.prepareAndAbort (0, Create (t, Apple, One))
            s.expectCells (t) ()
          }

          "ignore hold Apple at ts=0" in {
            val t = nextTable
            s.prepareAndAbort (0, Hold (t, Apple))
            s.expectCells (t) ()
          }

          "ignore update Apple::1 at ts=0" in {
            val t = nextTable
            s.prepareAndAbort (0, Update (t, Apple, One))
            s.expectCells (t) ()
          }

          "ignore delete Apple at ts=0" in {
            val t = nextTable
            s.prepareAndAbort (0, Delete (t, Apple))
            s.expectCells (t) ()
          }}}

      "when the table has Apple##ts::1" - {

        def newTableWithData = {
          val t = nextTable
          val ts = s.prepareAndCommit (0, Create (t, Apple, One))
          (t, ts)
        }

        "and a write aborts should" -  {

          "ignore update Apple::2 at ts+1" in {
            val (t, ts1) = newTableWithData
            s.prepareAndAbort (ts1+1, Update (t, Apple, Two))
            s.expectCells (t) (Apple##ts1::One)
          }

          "ignore delete Apple at ts+1" in {
            val (t, ts1) = newTableWithData
            s.prepareAndAbort (ts1+1, Delete (t, Apple))
            s.expectCells (t) (Apple##ts1::One)
          }}}}}}
