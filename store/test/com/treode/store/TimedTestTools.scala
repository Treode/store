package com.treode.store

import scala.util.Random

import com.treode.async.{Async, AsyncTestTools, CallbackCaptor, StubScheduler}
import org.scalatest.Assertions

import Assertions.{expectResult, fail}

private trait TimedTestTools extends AsyncTestTools {

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
       actual.expect (Collided (ks))

    def expectStale (implicit s: StubScheduler): Unit =
      actual.expect (Stale)
  }

  def Get (id: TableId, key: Bytes): ReadOp =
    ReadOp (id, key)
}

private object TimedTestTools extends TimedTestTools
