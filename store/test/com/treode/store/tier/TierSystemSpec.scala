package com.treode.store.tier

import scala.util.Random

import com.treode.async.Async
import com.treode.async.implicits._
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.disk.{Disks, DisksConfig, DiskGeometry}
import com.treode.disk.stubs.CrashChecks
import com.treode.store.{Bytes, StoreConfig}
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.FreeSpec

import Async.async
import TierTestTools._

class TierSystemSpec extends FreeSpec with CrashChecks {

  val ID = 0xC8

  implicit class RichRandom (random: Random) {

    def nextPut (nkeys: Int, nputs: Int): Seq [(Int, Int)] = {

      var keys = Set.empty [Int]
      def nextKey = {
        var key = random.nextInt (nkeys)
        while (keys contains key)
          key = random.nextInt (nkeys)
        keys += key
        key
      }

      def nextValue = random.nextInt (Int.MaxValue)
      Seq.fill (nputs) (nextKey, nextValue)
    }}

  implicit class RichTestTable (table: TestTable) {

    def putAll (kvs: (Int, Int)*): Async [Unit] =
      for ((key, value) <- kvs.latch.unit)
        table.put (key, value)

    def toSeq  (implicit scheduler: StubScheduler): Seq [(Int, Int)] =
      for (c <- table.iterator.toSeq; if c.value.isDefined)
        yield (c.key, c.value.get)

    def toMap (implicit scheduler: StubScheduler): Map [Int, Int] =
      toSeq.toMap

    def expectNone (key: Int) (implicit scheduler: StubScheduler): Unit =
      table.get (key) .expect (None)

    def expectValue (key: Int, value: Int) (implicit scheduler: StubScheduler): Unit =
      table.get (key) .expect (Some (value))

    def expectValues (kvs: (Int, Int)*) (implicit scheduler: StubScheduler): Unit =
      assertResult (kvs.sorted) (toSeq)
  }

  private def setup (
      disk: StubFile,
      geometry: DiskGeometry
  ) (implicit
      scheduler: StubScheduler,
      disksConfig: DisksConfig,
      storeConfig: StoreConfig
  ): TestTable = {
      implicit val recovery = Disks.recover()
      val _table = new TestRecovery (ID)
      val launch = recovery._attach (("a", disk, geometry)) .pass
      val table = _table.launch (launch) .pass
      launch.launch()
      table
  }

  private def recover (
      disk: StubFile
  ) (implicit
      scheduler: StubScheduler,
      disksConfig: DisksConfig,
      storeConfig: StoreConfig
  ): TestTable = {
    implicit val recovery = Disks.recover()
    val _table = new TestRecovery (ID)
    val launch = recovery._reattach (("a", disk)) .pass
    val table = _table.launch (launch) .pass
    launch.launch()
    table
  }

  def check (
      nkeys: Int,
      nrounds: Int,
      nbatch: Int
  ) (implicit
      geometry: DiskGeometry,
      random: Random,
      disksConfig: DisksConfig,
      storeConfig: StoreConfig
  ) = {

    implicit val disksConfig = TestDisksConfig()
    implicit val storeConfig = TestStoreConfig()
    val geometry = TestDiskGeometry()
    val tracker = new TrackingTable
    var file: StubFile = null

    setup { implicit scheduler =>
      file = StubFile (1<<20)
      val table = new TrackedTable (setup (file, geometry), tracker)
      (0 until nrounds) .async.foreach { _ =>
        table.putAll (random.nextPut (nkeys, nbatch): _*)
      }}

    .recover { implicit scheduler =>
      file = StubFile (file.data)
      val table = recover (file)
      tracker.check (table.toMap)
    }}

  "When given large limits for checkpointing and cleaning" - {

    implicit val disksConfig = TestDisksConfig()
    implicit val storeConfig = TestStoreConfig()
    implicit val geometry = TestDiskGeometry()

    "it can recover" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>
        check (100, 10, 10)
      }}

    "it can recover with lots of overwrites" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>
        check (30, 10, 10)
      }}

    "it can recover with very few overwrites" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>
        check (10000, 10, 10)
      }}}

  "When given a small threshold for checkpointing" - {

    implicit val disksConfig = TestDisksConfig (checkpointEntries = 57)
    implicit val storeConfig = TestStoreConfig()
    implicit val geometry = TestDiskGeometry()

    "it can recover" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>
        check (100, 40, 10)
      }}}
}
