package com.treode.store

import scala.util.Random

import com.treode.async.{Async, AsyncTestTools, CallbackCaptor, StubScheduler}
import com.treode.disk.{CellId, DisksConfig, DiskGeometry}
import org.scalatest.Assertions

import Assertions.{assertResult, fail}

private trait TimedTestTools extends AsyncTestTools {

  implicit class RichBytes (v: Bytes) {
    def ## (time: Int) = Cell (v, TxClock (time), None)
    def ## (time: TxClock) = Cell (v, time, None)
    def :: (time: Int) = Value (TxClock (time), Some (v))
    def :: (time: TxClock) = Value (time, Some (v))
    def :: (cell: Cell) = Cell (cell.key, cell.time, Some (v))
  }

  implicit class RichInt (v: Int) {
    def :: (cell: Cell) = Cell (cell.key, cell.time, Some (Bytes (v)))
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

  object TestDisksConfig {

    def apply (
      cell: CellId = 0,
        superBlockBits: Int = 12,
        maximumRecordBytes: Int = 1<<10,
        maximumPageBytes: Int = 1<<10,
        checkpointBytes: Int = Int.MaxValue,
        checkpointEntries: Int = Int.MaxValue,
        cleaningFrequency: Int = Int.MaxValue,
        cleaningLoad: Int = 1
    ): DisksConfig =
      DisksConfig (
          cell: CellId,
          superBlockBits,
          maximumRecordBytes,
          maximumPageBytes,
          checkpointBytes,
          checkpointEntries,
          cleaningFrequency,
          cleaningLoad)
  }

  object TestDiskGeometry {

    def apply (
        segmentBits: Int = 12,
        blockBits: Int = 6,
        diskBytes: Long = 1<<20
    ) (implicit
        config: DisksConfig
     ): DiskGeometry =
       DiskGeometry (
           segmentBits,
           blockBits,
           diskBytes)
  }

  def Get (id: TableId, key: Bytes): ReadOp =
    ReadOp (id, key)
}

private object TimedTestTools extends TimedTestTools
