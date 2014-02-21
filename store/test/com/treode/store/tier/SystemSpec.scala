package com.treode.store.tier

import java.nio.file.Paths
import scala.util.Random

import com.treode.async.{Async, AsyncConversions, AsyncTestTools, StubScheduler}
import com.treode.async.io.StubFile
import com.treode.disk.{Disks, DisksConfig, DiskGeometry}
import com.treode.store.{Bytes, StoreConfig}
import org.scalatest.FlatSpec

import Async.async
import AsyncConversions._
import AsyncTestTools._

class SystemSpec extends FlatSpec {

  implicit class RichRandom (random: Random) {

    def nextPut (nkeys: Int, nputs: Int): Seq [(Int, Int)] =
      Seq.fill (nputs) (random.nextInt (nkeys), random.nextInt (Int.MaxValue))
  }

  implicit class RichTestTable (table: TestTable) {

    def put (kvs: (Int, Int)*): Async [Unit] =
      kvs.latch.unit { case (key, value) => table.put (key, value) }

    def toSeq  (implicit scheduler: StubScheduler): Seq [(Int, Int)] =
      for (c <- table.iterator.toSeq; if c.value.isDefined)
        yield (c.key, c.value.get)

    def toMap (implicit scheduler: StubScheduler): Map [Int, Int] =
      toSeq.toMap

    def expectNone (key: Int) (implicit scheduler: StubScheduler): Unit =
      expectPass (None) (table.get (key))

    def expectValue (key: Int, value: Int) (implicit scheduler: StubScheduler): Unit =
      expectPass (Some (value)) (table.get (key))

    def expectValues (kvs: (Int, Int)*) (implicit scheduler: StubScheduler): Unit =
      expectResult (kvs.sorted) (toSeq)
  }

  private def setup (disk: StubFile, geometry: DiskGeometry) (
      implicit scheduler: StubScheduler, config: StoreConfig): TestTable = {

      implicit val disksConfig = DisksConfig (14, 1<<24, 1<<16, 10, 1)
      implicit val recovery = Disks.recover()
      val tableCb = TestTable.recover() .capture()
      recovery.attach (Seq ((Paths.get ("a"), disk, geometry))) .pass
      tableCb.passed
  }

  private def recover (disk: StubFile) (
      implicit scheduler: StubScheduler, storeConfig: StoreConfig): TestTable = {

    implicit val config = DisksConfig (14, 1<<24, 1<<16, 10, 1)
    implicit val recovery = Disks.recover()
    val tableCb = TestTable.recover() .capture()
    recovery.reattach (Seq ((Paths.get ("a"), disk))) .pass
    tableCb.passed
  }

  "It" should "work" in {

    implicit val disksConfig = DisksConfig (14, 1<<24, 1<<16, 10, 1)
    implicit val storeConfig = StoreConfig (4, 1 << 12)
    val geometry = DiskGeometry (20, 13, 1<<30)

    implicit val random = new Random (0)
    implicit val scheduler = StubScheduler.random (random)
    val disk = new StubFile
    val tracker = new TrackingTable

    {
      val _table = setup (disk, geometry)
      val table = new TrackedTable (_table, tracker)
      table.put (random.nextPut (10000, 1000): _*)
    }

    {
      val table = recover (disk)
      tracker.check (table.toMap)
    }}}
