package com.treode.store

import scala.language.implicitConversions
import scala.util.Random

import com.treode.async.{Async, AsyncTestTools, CallbackCaptor, StubScheduler}
import com.treode.cluster.StubHost
import com.treode.disk.{CellId, DisksConfig, DiskGeometry}
import org.scalatest.Assertions

import Assertions.{assertResult, fail}

private trait StoreTestTools extends AsyncTestTools {

  implicit def intToBytes (v: Int): Bytes =
    Bytes (v)

  implicit def longToBytes (v: Long): Bytes =
    Bytes (v)

  implicit def longToTxClock (v: Long): TxClock =
    new TxClock (v)

  implicit class RichBytes (v: Bytes) {
    def ## (time: Int) = Cell (v, time, None)
    def ## (time: TxClock) = Cell (v, time, None)
    def :: (time: Int) = Value (time, Some (v))
    def :: (time: TxClock) = Value (time, Some (v))
    def :: (cell: Cell) = Cell (cell.key, cell.time, Some (v))
  }

  implicit class RichInt (v: Int) {
    def :: (time: TxClock) = Value (time, Some (Bytes (v)))
    def :: (cell: Cell) = Cell (cell.key, cell.time, Some (Bytes (v)))
  }

  implicit class RichOption (v: Option [Bytes]) {
    def :: (time: Int) = Value (time, v)
    def :: (time: TxClock) = Value (time, v)
  }

  implicit class RichTxClock (v: TxClock) {
    def + (n: Int) = new TxClock (v.time + n)
    def - (n: Int) = new TxClock (v.time - n)
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

  object TestStoreConfig {

    def apply (
        priorValueEpoch: Epoch = Epoch.zero,
        lockSpaceBits: Int = 4,
        targetPageBytes: Int = 1<<10,
        rebalanceBytes: Int = Int.MaxValue,
        rebalanceEntries: Int = Int.MaxValue
    ): StoreConfig =
      StoreConfig (
          priorValueEpoch,
          lockSpaceBits,
          targetPageBytes,
          rebalanceBytes,
          rebalanceEntries)
  }

  def Get (id: TableId, key: Bytes): ReadOp =
    ReadOp (id, key)

  def settled (num: Int, h1: StubHost, h2: StubHost, h3: StubHost): Cohort =
    Cohort.settled (num, h1.localId, h2.localId, h3.localId)

  def moving (
      num: Int,
      origin: (StubHost, StubHost, StubHost),
      target: (StubHost, StubHost, StubHost)
  ): Cohort = {
    val (o1, o2, o3) = origin
    val (t1, t2, t3) = target
    Cohort (
        num,
        Set (o1.localId, o2.localId, o3.localId),
        Set (t1.localId, t2.localId, t3.localId))
  }}

private object StoreTestTools extends StoreTestTools
