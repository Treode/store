package com.treode.store.tier

import java.nio.file.Paths
import scala.util.Random

import com.treode.async.{Async, AsyncConversions, AsyncTestTools, StubScheduler}
import com.treode.async.io.StubFile
import com.treode.disk.{CrashChecks, Disks, DisksConfig, DiskGeometry}
import com.treode.store.{Bytes, StoreConfig}
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.PropSpec

import Async.async
import AsyncConversions._
import AsyncTestTools._

class SystemSpec extends PropSpec with CrashChecks {

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
      kvs.latch.unit { case (key, value) => table.put (key, value) }

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

  private def setup (disk: StubFile, geometry: DiskGeometry) (
      implicit scheduler: StubScheduler, config: StoreConfig): TestTable = {

      implicit val disksConfig = DisksConfig (0, 14, 1<<24, 1<<16, 10, 1)
      implicit val recovery = Disks.recover()
      val _table = new TestRecovery (ID)
      val files = Seq ((Paths.get ("a"), disk, geometry))
      val launch = recovery.attach (files) .pass
      _table.launch (launch) .pass
  }

  private def recover (disk: StubFile) (
      implicit scheduler: StubScheduler, storeConfig: StoreConfig): TestTable = {

    implicit val config = DisksConfig (0, 14, 1<<24, 1<<16, 10, 1)
    implicit val recovery = Disks.recover()
    val _table = new TestRecovery (ID)
    val files = Seq ((Paths.get ("a"), disk))
    val launch = recovery.reattach (files) .pass
    _table.launch (launch) .pass
  }

  def check (nkeys: Int, nrounds: Int, nbatch: Int) (implicit random: Random) = {

    implicit val disksConfig = DisksConfig (0, 10, 1<<30, 1<<30, 1<<30, 1)
    implicit val storeConfig = StoreConfig (8, 1 << 10)
    val geometry = DiskGeometry (14, 8, 1<<30)
    val disk = new StubFile () (null)
    val tracker = new TrackingTable

    setup { implicit scheduler =>
      disk.scheduler = scheduler
      val table = new TrackedTable (setup (disk, geometry), tracker)
      (0 until nrounds) .async.foreach { _ =>
        table.putAll (random.nextPut (nkeys, nbatch): _*)
      }}

    .recover { implicit scheduler =>
      disk.scheduler = scheduler
      val table = recover (disk)
      tracker.check (table.toMap)
    }}

  property ("It can recover", Intensive, Periodic) {
    forAllCrashes { implicit random =>
      check (100, 10, 10)
    }}

  property ("It can recover with lots of overwrites", Intensive, Periodic) {
    forAllCrashes { implicit random =>
      check (30, 10, 10)
    }}

  property ("It can recover with very few overwrites", Intensive, Periodic) {
    forAllCrashes { implicit random =>
      check (10000, 10, 10)
    }}}
