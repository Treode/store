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

import java.nio.file.{Path, Paths}
import scala.language.implicitConversions
import scala.util.Random

import com.treode.async.{Async, AsyncIterator, BatchIterator}
import com.treode.async.stubs.{CallbackCaptor, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.cluster.HostId
import com.treode.cluster.stubs.{StubNetwork, StubPeer}
import com.treode.disk.DriveGeometry
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

  def newKit() = {
    val random = new Random (0)
    val scheduler = StubScheduler.random (random)
    val network = StubNetwork (random)
    (random, scheduler, network)
  }

  def settled (hosts: StubPeer*): Cohort =
    Cohort.settled (hosts map (_.localId): _*)

  def issuing (origin: StubPeer*) (target: StubPeer*): Cohort =
    Cohort.issuing (origin map (_.localId): _*) (target map (_.localId): _*)

  def moving (origin: StubPeer*) (target: StubPeer*): Cohort =
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

  def assertCells (expected: Cell*) (actual: BatchIterator [Cell]) (implicit scheduler: StubScheduler) {
    val _actual = actual.toSeq.expectPass()
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
    def ## (time: Int) = Cell (v, time, None)
    def ## (time: TxClock) = Cell (v, time, None)
    def :: (time: Int) = Value (time, Some (v))
    def :: (time: TxClock) = Value (time, Some (v))
    def :: (cell: Cell) = Cell (cell.key, cell.time, Some (v))
  }

  implicit class RichLong (v: Long) {
    def ## (time: Int) = Cell (v, time, None)
    def ## (time: TxClock) = Cell (v, time, None)
    def :: (time: Int) = Value (time, Some (v))
    def :: (time: TxClock) = Value (time, Some (v))
    def :: (cell: Cell) = Cell (cell.key, cell.time, Some (v))
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

    def scan (table: TableId): CellIterator2 =
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
