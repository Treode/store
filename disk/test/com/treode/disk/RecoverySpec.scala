package com.treode.disk

import java.nio.file.Paths

import com.treode.async.{AsyncTestTools, StubScheduler}
import com.treode.async.implicits._
import com.treode.async.io.StubFile
import org.scalatest.FreeSpec

import DiskTestTools._

class RecoverySpec extends FreeSpec {

  implicit val config = TestDisksConfig()
  val geom = TestDiskGeometry()
  val record = RecordDescriptor (0x1BF6DBABE6A70060L, DiskPicklers.int)

  "Recovery.replay should" - {

    "allow registration of a record descriptor" in {
      implicit val scheduler = StubScheduler.random()
      val recovery = Disks.recover()
      recovery.replay (record) (_ => ())
    }

    "reject double registration of a record descriptor" in {
      implicit val scheduler = StubScheduler.random()
      val recovery = Disks.recover()
      recovery.replay (record) (_ => ())
      intercept [IllegalArgumentException] {
        recovery.replay (record) (_ => ())
      }}

    "reject registration of a record descriptor after attach" in {
      implicit val scheduler = StubScheduler.random()
      val file = new StubFile
      val recovery = Disks.recover()
      recovery.attachAndWait (("a", file, geom)) .pass
      intercept [IllegalArgumentException] {
        recovery.replay (record) (_ => ())
      }}

    "reject registration of a record descriptor after reattach" in {
      implicit val scheduler = StubScheduler.random()
      val file = new StubFile
      var recovery = Disks.recover()
      recovery.attachAndLaunch (("a", file, geom))
      recovery = Disks.recover()
      recovery.reattachAndLaunch (("a", file))
      intercept [IllegalArgumentException] {
        recovery.replay (record) (_ => ())
      }}}

  "Recovery.attach should" - {

    "allow attaching an item" in {
      implicit val scheduler = StubScheduler.random()
      val file = new StubFile
      val recovery = Disks.recover()
      recovery.attachAndLaunch (("a", file, geom))
    }

    "allow attaching multiple items" in {
      implicit val scheduler = StubScheduler.random()
      val file1 = new StubFile
      val file2 = new StubFile
      val recovery = Disks.recover()
      recovery.attachAndLaunch (("a", file1, geom), ("b", file2, geom))
    }

    "reject attaching no items" in {
      implicit val scheduler = StubScheduler.random()
      val recovery = Disks.recover()
      recovery.attachAndWait() .fail [IllegalArgumentException]
    }

    "reject attaching the same path multiply" in {
      implicit val scheduler = StubScheduler.random()
      val file = new StubFile
      val recovery = Disks.recover()
      recovery
          .attachAndWait (("a", file, geom), ("a", file, geom))
          .fail [IllegalArgumentException]
    }

    "pass through an exception from DiskDrive.init" in {
      implicit val scheduler = StubScheduler.random()
      val file = new StubFile
      val recovery = Disks.recover()
      file.stop = true
      val cb = recovery.attachAndCapture (("a", file, geom))
      scheduler.runTasks()
      file.stop = false
      while (file.hasLast)
        file.last.fail (new Exception)
      scheduler.runTasks()
      cb.failed [Exception]
    }}

  "Recovery.reattach" - {

    "in general should" - {

      "allow reattaching an item" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        var recovery = Disks.recover()
        recovery.attachAndLaunch (("a", file, geom))
        recovery = Disks.recover()
        recovery.reattachAndLaunch (("a", file))
      }

      "allow reattaching multiple items" in {
        implicit val scheduler = StubScheduler.random()
        val file1 = new StubFile
        val file2 = new StubFile
        var recovery = Disks.recover()
        recovery.attachAndLaunch (("a", file1, geom), ("b", file2, geom))
        recovery = Disks.recover()
        recovery.reattachAndLaunch (("a", file1), ("b", file2))
      }

      "reject reattaching no items" in {
        implicit val scheduler = StubScheduler.random()
        val recovery = Disks.recover()
        recovery.reattachAndWait() .fail [IllegalArgumentException]
      }

      "reject reattaching the same path multiply" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        var recovery = Disks.recover()
        recovery.attachAndLaunch (("a", file, geom))
        recovery = Disks.recover()
        recovery
            .reattachAndWait (("a", file), ("a", file))
            .fail [IllegalArgumentException]
      }

      "pass through an exception from chooseSuperBlock" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        val recovery = Disks.recover()
        recovery.reattachAndWait (("a", file)) .fail [NoSuperBlocksException]
      }

      "pass through an exception from verifyReattachment" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        var recovery = Disks.recover()
        recovery.attachAndLaunch (("a", file, geom))
        val config2 = TestDisksConfig (cell = 1)
        recovery = Disks.recover () (scheduler, config2)
        recovery.reattachAndWait (("a", file)) .fail [CellMismatchException]
      }}

    "when given opened files should" - {

      "require the given file paths match the boot blocks disks" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        val file2 = new StubFile
        var recovery = Disks.recover()
        recovery.attachAndLaunch (("a", file, geom), ("b", file2, geom))
        recovery = Disks.recover()
        recovery.reattachAndWait (("a", file)) .fail [MissingDisksException]
      }}

    "when given unopened paths should" - {

      "pass when given all of the items" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        val file2 = new StubFile
        var recovery = Disks.recover()
        recovery.attachAndLaunch (("a", file, geom), ("b", file2, geom))
        recovery = Disks.recover()
        recovery.reopenAndLaunch ("a", "b") (("a", file), ("b", file2))
      }

      "pass when given a subset of the items" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        val file2 = new StubFile
        var recovery = Disks.recover()
        recovery.attachAndLaunch (("a", file, geom), ("b", file2, geom))
        recovery = Disks.recover()
        recovery.reopenAndLaunch ("a") (("a", file), ("b", file2))
      }

      "fail when given extra uninitialized items" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        val file2 = new StubFile
        val file3 = new StubFile
        var recovery = Disks.recover()
        recovery.attachAndLaunch (("a", file, geom), ("b", file2, geom))
        recovery = Disks.recover()
        recovery
            .reopenAndWait ("a", "c") (("a", file), ("b", file2), ("c", file3))
            .fail [InconsistentSuperBlocksException]
      }

      "fail when given extra initialized items" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        val file2 = new StubFile
        val file3 = new StubFile
        var recovery = Disks.recover()
        recovery.attachAndLaunch (("a", file, geom), ("b", file2, geom))
        recovery = Disks.recover()
        recovery.attachAndLaunch (("c", file3, geom))
        recovery = Disks.recover()
        recovery
            .reopenAndWait ("a", "c") (("a", file), ("b", file2), ("c", file3))
            .fail [ExtraDisksException]
      }}}}
