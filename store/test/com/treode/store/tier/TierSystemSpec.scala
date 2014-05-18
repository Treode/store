package com.treode.store.tier

import scala.util.Random

import com.treode.async.Async
import com.treode.async.implicits._
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.disk.stubs.{CrashChecks, StubDisk, StubDiskDrive}
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
      checkpoint: Double,
      compaction: Double,
      diskDrive: StubDiskDrive
  ) (implicit
      random: Random,
      scheduler: StubScheduler,
      storeConfig: StoreConfig
  ): TestTable = {
      implicit val recovery = StubDisk.recover (checkpoint, compaction)
      val _table = new TestRecovery (ID)
      val launch = recovery.attach (diskDrive) .pass
      val table = _table.launch (launch) .pass
      launch.launch()
      table
  }

  private def recover (
      checkpoint: Double,
      compaction: Double,
      diskDrive: StubDiskDrive
  ) (implicit
      random: Random,
      scheduler: StubScheduler,
      storeConfig: StoreConfig
  ): TestTable = {
    implicit val recovery = StubDisk.recover (checkpoint, compaction)
    val _table = new TestRecovery (ID)
    val launch = recovery.reattach (diskDrive) .pass
    val table = _table.launch (launch) .pass
    launch.launch()
    table
  }

  def check (
      checkpoint: Double,
      compaction: Double,
      nkeys: Int,
      nrounds: Int,
      nbatch: Int
  ) (implicit
      random: Random,
      storeConfig: StoreConfig
  ) = {

      implicit val storeConfig = TestStoreConfig()
      val tracker = new TrackingTable
      val diskDrive = new StubDiskDrive

      crash.info (s"check ($checkpoint, $compaction, $nkeys, $nrounds, $nbatch)")

      .setup { implicit scheduler =>
        val rawtable = setup (checkpoint, compaction, diskDrive)
        val table = new TrackedTable (rawtable, tracker)
        (0 until nrounds) .async.foreach { _ =>
          table.putAll (random.nextPut (nkeys, nbatch): _*)
        }}

      .recover { implicit scheduler =>
        val table = recover (checkpoint, compaction, diskDrive)
        tracker.check (table.toMap)
      }}

  "The TierTable when" - {

    implicit val storeConfig = TestStoreConfig()

    for { (name, checkpoint) <- Seq (
        "not checkpointed at all"   -> 0.0,
        "checkpointed occasionally" -> 0.01,
        "checkpointed frequently"   -> 0.1)
    } s"$name and" - {

      for { (name, compaction) <- Seq (
          "not compacted at all"   -> 0.0,
          "compacted occasionally" -> 0.01,
          "compacted frequently"   -> 0.1)
    } s"$name should" - {

      for { (name, (nkeys, nrounds, nbatch)) <- Seq (
          "recover from a crash"             -> (100, 10, 10),
          "recover with lots of overwrites"  -> (30, 10, 10),
          "recover with very few overwrites" -> (10000, 10, 10))
      } name taggedAs (Intensive, Periodic) in {

        forAllCrashes { implicit random =>
          check (checkpoint, compaction, nkeys, nrounds, nbatch)
        }}}}}}
