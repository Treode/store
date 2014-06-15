package com.treode.store

import java.nio.file.{Path, Paths}
import scala.language.implicitConversions
import scala.util.Random

import com.treode.async.{Async, AsyncIterator}
import com.treode.async.stubs.{CallbackCaptor, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.cluster.HostId
import com.treode.disk.{DiskConfig, DriveGeometry}
import org.scalatest.Assertions

import Assertions.{assertResult, fail}

private trait StoreTestTools {

  private val fixedLongLong = {
    import StorePicklers._
    tuple (fixedLong, fixedLong)
  }

  val MinStart = Bound.Inclusive (Key.MinValue)

  val AllSlices = Slice (0, 1)

  val AllTimes = Window.Through (Bound.Inclusive (TxClock.MaxValue), TxClock.MinValue)

  def Get (id: TableId, key: Bytes): ReadOp =
    ReadOp (id, key)

  def settled (hosts: StubStoreHost*): Cohort =
    Cohort.settled (hosts map (_.localId): _*)

  def issuing (origin: StubStoreHost*) (target: StubStoreHost*): Cohort =
    Cohort.issuing (origin map (_.localId): _*) (target map (_.localId): _*)

  def moving (origin: StubStoreHost*) (target: StubStoreHost*): Cohort =
    Cohort.moving (origin map (_.localId): _*) (target map (_.localId): _*)

  def testStringOf (cell: Cell): String = {
    val k = cell.key.string
    val t = cell.time.time
    cell.value match {
      case Some (v) => s"$k##$t::${v.int}"
      case None => s"$k##$t::_"
    }}

  def testStringOf (cells: Seq [Cell]): String =
    cells.map (testStringOf _) .mkString ("[", ", ", "]")

  def assertCells (expected: Cell*) (actual: AsyncIterator [Cell]) (implicit scheduler: StubScheduler) {
    val _actual = actual.toSeq.pass
    if (expected != _actual)
      fail (s"Expected ${testStringOf (expected)}, found ${testStringOf (_actual)}")
  }

  implicit def intToBytes (v: Int): Bytes =
    Bytes (v)

  implicit def longToBytes (v: Long): Bytes =
    Bytes (v)

  implicit def longToTxClock (v: Long): TxClock =
    new TxClock (v)

  implicit def longToTxId (v: Long): TxId =
    TxId (Bytes (v), 0)

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

  implicit class RichRandom (random: Random) {

    def nextTxId: TxId =
      TxId (Bytes (fixedLongLong, (random.nextLong, random.nextLong)), 0)
  }

  implicit class RichStore (store: Store) {

    def scan (table: TableId): CellIterator =
      store.scan (table, MinStart, AllTimes, AllSlices)

    def expectCells (table: TableId) (expected: Cell*) (implicit scheduler: StubScheduler) =
      assertCells (expected: _*) (store.scan (table))
  }

  implicit class RichTxClock (v: TxClock) {
    def + (n: Int) = new TxClock (v.time + n)
    def - (n: Int) = new TxClock (v.time - n)
  }

  implicit class StoreTestingAsync [A] (async: Async [A]) {

    def passOrTimeout(): Unit =
      try {
        async.await()
      } catch {
        case _: TimeoutException => ()
      }}

  implicit class StoreTestingCallbackCaptor [A] (cb: CallbackCaptor [A]) {

    def passedOrTimedout: Unit =
      assert (
          cb.hasPassed || cb.hasFailed [TimeoutException],
          s"Expected success or timeout, found $cb")
  }}

private object StoreTestTools extends StoreTestTools
