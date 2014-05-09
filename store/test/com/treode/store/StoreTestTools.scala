package com.treode.store

import java.nio.file.{Path, Paths}
import scala.language.implicitConversions
import scala.util.Random

import com.treode.async.Async
import com.treode.async.stubs.{CallbackCaptor, StubScheduler}
import com.treode.cluster.HostId
import com.treode.disk.{CellId, DisksConfig, DiskGeometry}
import org.scalatest.Assertions

import Assertions.{assertResult, fail}

private trait StoreTestTools {

  implicit def intToBytes (v: Int): Bytes =
    Bytes (v)

  implicit def longToBytes (v: Long): Bytes =
    Bytes (v)

  implicit def longToTxClock (v: Long): TxClock =
    new TxClock (v)

  implicit def stringToPath (path: String): Path =
    Paths.get (path)

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

  object TestStoreConfig {

    def apply (
        priorValueEpoch: Epoch = Epoch.zero,
        falsePositiveProbability: Double = 0.01,
        lockSpaceBits: Int = 4,
        targetPageBytes: Int = 1<<10,
        rebalanceBytes: Int = Int.MaxValue,
        rebalanceEntries: Int = Int.MaxValue
    ): StoreConfig =
      StoreConfig (
          priorValueEpoch,
          falsePositiveProbability,
          lockSpaceBits,
          targetPageBytes,
          rebalanceBytes,
          rebalanceEntries)
  }

  def Get (id: TableId, key: Bytes): ReadOp =
    ReadOp (id, key)

  def settled (hosts: StubStoreHost*): Cohort =
    Cohort.settled (hosts map (_.localId): _*)

  def issuing (origin: StubStoreHost*) (target: StubStoreHost*): Cohort =
    Cohort.issuing (origin map (_.localId): _*) (target map (_.localId): _*)

  def moving (origin: StubStoreHost*) (target: StubStoreHost*): Cohort =
    Cohort.moving (origin map (_.localId): _*) (target map (_.localId): _*)
}

private object StoreTestTools extends StoreTestTools
