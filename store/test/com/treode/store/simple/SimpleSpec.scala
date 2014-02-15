package com.treode.store.simple

import java.nio.file.Paths
import scala.util.Random

import com.treode.async._
import com.treode.async.io.StubFile
import com.treode.disk.{Disks, DisksConfig, DiskGeometry}
import com.treode.store.{Bytes, StoreConfig}
import org.scalatest.FlatSpec

class SimpleSpec extends FlatSpec {

  implicit class RichRandom (random: Random) {

    def nextPut (nkeys: Int, nputs: Int): Seq [(Int, Int)] =
      Seq.fill (nputs) (random.nextInt (nkeys), random.nextInt (Int.MaxValue))
  }

  implicit class RichTestTable (table: TestTable) {

    def getAndPass (key: Int) (implicit scheduler: StubScheduler): Option [Int] =
      CallbackCaptor.pass [Option [Int]] (table.get (key, _))

    def putAndPass (kvs: (Int, Int)*) (implicit scheduler: StubScheduler) {
      CallbackCaptor.pass [Unit] { cb =>
        val latch = Latch.unit (kvs.size, cb)
        for ((key, value) <- kvs)
          table.put (key, value, latch)
      }}

    def deleteAndPass (ks: Int*) (implicit scheduler: StubScheduler) {
      CallbackCaptor.pass [Unit] { cb =>
        for (key <- ks)
          table.delete (key, cb)
      }}

    def toMap (implicit scheduler: StubScheduler): Map [Int, Int] = {
      val builder = Map.newBuilder [Int, Int]
      CallbackCaptor.pass [Unit] { cb =>
        table.iterator (continue (cb) { iter: TestIterator =>
          AsyncIterator.foreach (iter, cb) { case (cell, cb) =>
            invoke (cb) {
              if (cell.value.isDefined)
                builder += cell.key -> cell.value.get
            }}})
      }
      builder.result
    }

    def toSeq  (implicit scheduler: StubScheduler): Seq [(Int, Int)] = {
      val builder = Seq.newBuilder [(Int, Int)]
      CallbackCaptor.pass [Unit] { cb =>
        table.iterator (continue (cb) { iter: TestIterator =>
          AsyncIterator.foreach (iter, cb) { case (cell, cb) =>
            invoke (cb) {
              if (cell.value.isDefined)
                builder += cell.key -> cell.value.get
            }}})
      }
      builder.result
    }

    def expectNone (key: Int) (implicit scheduler: StubScheduler): Unit =
      expectResult (None) (getAndPass (key))

    def expectValue (key: Int, value: Int) (implicit scheduler: StubScheduler): Unit =
      expectResult (Some (value)) (getAndPass (key))

    def expectValues (kvs: (Int, Int)*) (implicit scheduler: StubScheduler): Unit =
      expectResult (kvs.sorted) (toSeq)
  }

  private def setup (disk: StubFile, geometry: DiskGeometry) (
      implicit scheduler: StubScheduler, config: StoreConfig): TestTable = {

      implicit val disksConfig = DisksConfig (14, 1<<24, 1<<16, 10, 1)
      implicit val recovery = Disks.recover()
      val tableCb = CallbackCaptor [TestTable]
      TestTable.recover (tableCb)
      val disksCb = CallbackCaptor [Disks]
      recovery.attach (Seq ((Paths.get ("a"), disk, geometry)), disksCb)
      scheduler.runTasks()
      disksCb.passed
      tableCb.passed
  }

  private def recover (disk: StubFile) (
      implicit scheduler: StubScheduler, storeConfig: StoreConfig): TestTable = {

    implicit val config = DisksConfig (14, 1<<24, 1<<16, 10, 1)
    implicit val recovery = Disks.recover()
    val tableCb = CallbackCaptor [TestTable]
    TestTable.recover (tableCb)
    val disksCb = CallbackCaptor [Disks]
    recovery.reattach (Seq ((Paths.get ("a"), disk)), disksCb)
    scheduler.runTasks()
    disksCb.passed
    tableCb.passed
  }

  "It" should "work" in {

    implicit val disksConfig = DisksConfig (14, 1<<24, 1<<16, 10, 1)
    implicit val storeConfig = new StoreConfig (1 << 12)
    val geometry = DiskGeometry (20, 13, 1<<30)

    implicit val random = new Random (0)
    implicit val scheduler = StubScheduler.random (random)
    val disk = new StubFile
    val tracker = new TrackingTable

    {
      val _table = setup (disk, geometry)
      val table = new TrackedTable (_table, tracker)
      table.putAndPass (random.nextPut (10000, 1000): _*)
    }

    {
      val table = recover (disk)
      tracker.check (table.toMap)
    }}}
