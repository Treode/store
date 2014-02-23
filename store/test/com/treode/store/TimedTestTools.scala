package com.treode.store

import scala.util.Random

import com.treode.async.{Async, AsyncTestTools, CallbackCaptor, StubScheduler}
import com.treode.store.locks.LockSet
import org.scalatest.Assertions

import AsyncTestTools._
import Assertions.{expectResult, fail}

private trait TimedTestTools {

  implicit class RichBytes (v: Bytes) {
    def ## (time: Int) = TimedCell (v, TxClock (time), None)
    def ## (time: TxClock) = TimedCell (v, time, None)
    def :: (time: Int) = Value (TxClock (time), Some (v))
    def :: (time: TxClock) = Value (time, Some (v))
    def :: (cell: TimedCell) = TimedCell (cell.key, cell.time, Some (v))
  }

  implicit class RichOption (v: Option [Bytes]) {
    def :: (time: Int) = Value (TxClock (time), v)
    def :: (time: TxClock) = Value (time, v)
  }

  implicit class RichTxClock (v: TxClock) {
    def + (n: Int) = TxClock (v.time + n)
    def - (n: Int) = TxClock (v.time - n)
  }

  implicit class RichTimedTable (t: TimedTable) {

    def getAndExpect (key: Bytes, time: TxClock) (expected: TimedCell) {
      val cb = CallbackCaptor [TimedCell]
      t.get (key, time, cb)
      expectResult (expected) (cb.passed)
    }

    def putAndPass (key: Bytes, time: TxClock, value: Option [Bytes]) {
      val cb = CallbackCaptor [Unit]
      t.put (key, time, value, cb)
      cb.passed
    }}

  implicit class RichPrepareResult (actual: Async [PrepareResult]) {
    import PrepareResult._

    def expectPrepared (implicit s: StubScheduler): (TxClock, LockSet) =
      actual.pass match {
        case Prepared (vt, locks) =>
          (vt, locks)
        case _ =>
          fail (s"Expected Written, found ${actual}")
          throw new Exception
      }

    def expectCollided (ks: Int*) (implicit s: StubScheduler): Unit =
      expectPass (Collided (ks)) (actual)

    def expectStale (implicit s: StubScheduler): Unit =
      expectPass (Stale) (actual)

    def abort() (implicit s: StubScheduler) {
      val (vt, locks) = expectPrepared
      locks.release()
    }}

  implicit class RichWriteResult (actual: Async [WriteResult]) {
    import WriteResult._

    def expectWritten (implicit s: StubScheduler): TxClock =
      actual.pass match {
        case Written (wt) =>
          wt
        case _ =>
          fail (s"Expected Written, found ${actual}")
          throw new Exception
      }

    def expectCollided (ks: Int*) (implicit s: StubScheduler): Unit =
      expectPass (Collided (ks)) (actual)

    def expectStale (implicit s: StubScheduler): Unit =
      expectPass (Stale) (actual)
  }

  def nextTable = TableId (Random.nextLong)

  def Get (id: TableId, key: Bytes): ReadOp =
    ReadOp (id, key)
}

private object TimedTestTools extends TimedTestTools
