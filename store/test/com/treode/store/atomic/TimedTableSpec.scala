package com.treode.store.atomic

import java.nio.file.Paths

import com.treode.async.StubScheduler
import com.treode.async.io.StubFile
import com.treode.disk.{Disks, DisksConfig, DiskGeometry}
import com.treode.store._
import org.scalatest.FreeSpec

import Fruits._
import TimedTable.keyToBytes
import TimedTestTools._

class TimedTableSpec extends FreeSpec {

  val ID = 0xCA

  private def newTable(): (StubScheduler, TimedTable) = {
    implicit val scheduler = StubScheduler.random()
    implicit val disksConfig = TestDisksConfig()
    implicit val recovery = Disks.recover()
    val file = new StubFile
    val geometry = TestDiskGeometry()
    val item = (Paths.get ("a"), file, geometry)
    val launch = recovery.attach (Seq (item)) .pass
    launch.launch()
    implicit val disks = launch.disks
    implicit val storeConfig = StoreConfig (8, 1<<20)
    val table = TimedTable (ID)
    (scheduler, table)
  }

  def expectCells (cs: Cell*) (t: TimedTable) (implicit s: StubScheduler): Unit =
    assertResult (cs) (t.iterator.toSeq)

  "TimedTable.keyToBytes should" - {

    "preserve the sort of the embedded key" in {
      assert (Boysenberry < Grape)
      assert (Bytes (Bytes.pickler, Boysenberry) > Bytes (Bytes.pickler, Grape))
      assert (keyToBytes (Boysenberry, 0) < keyToBytes (Grape, 0))
    }

    "reverse the sort order of time" in {
      assert (keyToBytes (Grape, 1) < keyToBytes (Grape, 0))
    }}

  "When a TimedTable is empty, it should" - {

    "get Apple##0 for Apple##1" in {
      implicit val (s, t) = newTable()
      t.get (Apple, 1) expect (0::None)
    }

    "put Apple##1::1" in {
      implicit val (s, t) = newTable()
      t.put (Apple, 1, 1)
      t.iterator.toSeq
      expectCells (Apple##1::1) (t)
    }}

  "When a TimedTable has Apple##7::1, it should " - {

    def newTableWithData() = {
      val (s, t) = newTable()
      t.put (Apple, 7, 1)
      (s, t)
    }

    "find 7::1 for Apple##8" in {
      implicit val (s, t) = newTableWithData()
      t.get (Apple, 8) expect (7::1)
    }

    "find 7::1 for Apple##7" in {
      implicit val (s, t) = newTableWithData()
      t.get (Apple, 7) expect (7::1)
    }

    "find 0::None for Apple##6" in {
      implicit val (s, t) = newTableWithData()
      t.get (Apple, 6) expect (0::None)
    }

    "put 11::2" in {
      implicit val (s, t) = newTableWithData()
      t.put (Apple, 11, 2)
      expectCells (Apple##11::2, Apple##7::1) (t)
    }

    "put Apple##3::2" in {
      implicit val (s, t) = newTableWithData()
      t.put (Apple, 3, 2)
      expectCells (Apple##7::1, Apple##3::2) (t)
    }}

  "When a TimedTable has Apple##14::2 and Apple##7::1, it should" -  {

    def newTableWithData() = {
      val (s, t) = newTable()
      t.put (Apple, 7, 1)
      t.put (Apple, 14, 2)
      (s, t)
    }

    "find 14::2 for Apple##15" in {
      implicit val (s, t) = newTableWithData()
      t.get (Apple, 15) expect (14::2)
    }

    "find 14::2 for Apple##14" in {
      implicit val (s, t) = newTableWithData()
      t.get (Apple, 14) expect (14::2)
    }

    "find 7::1 for Apple##13" in {
      implicit val (s, t) = newTableWithData()
      t.get (Apple, 13) expect (7::1)
    }

    "find 7::1 for Apple##8" in {
      implicit val (s, t) = newTableWithData()
      t.get (Apple, 8) expect (7::1)
    }

    "find 7::1 for Apple##7" in {
      implicit val (s, t) = newTableWithData()
      t.get (Apple, 7) expect (7::1)
    }

    "find 0::None for Apple##6" in {
      implicit val (s, t) = newTableWithData()
      t.get (Apple, 6) expect (0::None)
    }}}
