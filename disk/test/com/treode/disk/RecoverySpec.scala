package com.treode.disk

import java.nio.file.Paths

import com.treode.async.implicits._
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import org.scalatest.FreeSpec

import DiskTestTools._

class RecoverySpec extends FreeSpec {

  implicit val config = DiskTestConfig()
  val geom = DiskGeometry.test()
  val record = RecordDescriptor (0x1BF6DBABE6A70060L, DiskPicklers.int)

  "Recovery.replay should" - {

    "allow registration of a record descriptor" in {
      implicit val scheduler = StubScheduler.random()
      val recovery = Disk.recover()
      recovery.replay (record) (_ => ())
    }

    "reject double registration of a record descriptor" in {
      implicit val scheduler = StubScheduler.random()
      val recovery = Disk.recover()
      recovery.replay (record) (_ => ())
      intercept [IllegalArgumentException] {
        recovery.replay (record) (_ => ())
      }}

    "reject registration of a record descriptor after attach" in {
      implicit val scheduler = StubScheduler.random()
      val file = StubFile()
      val recovery = Disk.recover()
      recovery.attachAndWait (("a", file, geom)) .pass
      intercept [IllegalArgumentException] {
        recovery.replay (record) (_ => ())
      }}

    "reject registration of a record descriptor after reattach" in {
      implicit val scheduler = StubScheduler.random()
      val file = StubFile()
      var recovery = Disk.recover()
      recovery.attachAndLaunch (("a", file, geom))
      recovery = Disk.recover()
      recovery.reattachAndLaunch (("a", file))
      intercept [IllegalArgumentException] {
        recovery.replay (record) (_ => ())
      }}}

  "Recovery.attach should" - {

    "allow attaching an item" in {
      implicit val scheduler = StubScheduler.random()
      val file = StubFile()
      val recovery = Disk.recover()
      recovery.attachAndLaunch (("a", file, geom))
    }

    "allow attaching multiple items" in {
      implicit val scheduler = StubScheduler.random()
      val file1 = StubFile()
      val file2 = StubFile()
      val recovery = Disk.recover()
      recovery.attachAndLaunch (("a", file1, geom), ("b", file2, geom))
    }

    "reject attaching no items" in {
      implicit val scheduler = StubScheduler.random()
      val recovery = Disk.recover()
      recovery.attachAndWait() .fail [IllegalArgumentException]
    }

    "reject attaching the same path multiply" in {
      implicit val scheduler = StubScheduler.random()
      val file = StubFile()
      val recovery = Disk.recover()
      recovery
          .attachAndWait (("a", file, geom), ("a", file, geom))
          .fail [IllegalArgumentException]
    }

    "pass through an exception from DiskDrive.init" in {
      implicit val scheduler = StubScheduler.random()
      val file = StubFile()
      val recovery = Disk.recover()
      file.stop = true
      val cb = recovery.attachAndCapture (("a", file, geom))
      scheduler.run()
      file.stop = false
      while (file.hasLast)
        file.last.fail (new Exception)
      scheduler.run()
      cb.failed [Exception]
    }}

  "Recovery.reattach" - {

    "in general should" - {

      "allow reattaching an item" in {
        implicit val scheduler = StubScheduler.random()
        val file = StubFile()
        var recovery = Disk.recover()
        recovery.attachAndLaunch (("a", file, geom))
        recovery = Disk.recover()
        recovery.reattachAndLaunch (("a", file))
      }

      "allow reattaching multiple items" in {
        implicit val scheduler = StubScheduler.random()
        val file1 = StubFile()
        val file2 = StubFile()
        var recovery = Disk.recover()
        recovery.attachAndLaunch (("a", file1, geom), ("b", file2, geom))
        recovery = Disk.recover()
        recovery.reattachAndLaunch (("a", file1), ("b", file2))
      }

      "reject reattaching no items" in {
        implicit val scheduler = StubScheduler.random()
        val recovery = Disk.recover()
        recovery.reattachAndWait() .fail [IllegalArgumentException]
      }

      "reject reattaching the same path multiply" in {
        implicit val scheduler = StubScheduler.random()
        val file = StubFile()
        var recovery = Disk.recover()
        recovery.attachAndLaunch (("a", file, geom))
        recovery = Disk.recover()
        recovery
            .reattachAndWait (("a", file), ("a", file))
            .fail [IllegalArgumentException]
      }

      "pass through an exception from chooseSuperBlock" in {
        implicit val scheduler = StubScheduler.random()
        val file = StubFile()
        val recovery = Disk.recover()
        recovery.reattachAndWait (("a", file)) .fail [NoSuperBlocksException]
      }

      "pass through an exception from verifyReattachment" in {
        implicit val scheduler = StubScheduler.random()
        val file1 = StubFile()
        val file2 = StubFile()
        var recovery = Disk.recover()
        recovery.attachAndLaunch (("a", file1, geom), ("b", file2, geom))
        recovery = Disk.recover ()
        recovery.reattachAndWait (("a", file1)) .fail [MissingDisksException]

      }}

    "when given opened files should" - {

      "require the given file paths match the boot blocks disks" in {
        implicit val scheduler = StubScheduler.random()
        val file = StubFile()
        val file2 = StubFile()
        var recovery = Disk.recover()
        recovery.attachAndLaunch (("a", file, geom), ("b", file2, geom))
        recovery = Disk.recover()
        recovery.reattachAndWait (("a", file)) .fail [MissingDisksException]
      }}

    "when given unopened paths should" - {

      "pass when given all of the items" in {
        implicit val scheduler = StubScheduler.random()
        val file = StubFile()
        val file2 = StubFile()
        var recovery = Disk.recover()
        recovery.attachAndLaunch (("a", file, geom), ("b", file2, geom))
        recovery = Disk.recover()
        recovery.reopenAndLaunch ("a", "b") (("a", file), ("b", file2))
      }

      "pass when given a subset of the items" in {
        implicit val scheduler = StubScheduler.random()
        val file = StubFile()
        val file2 = StubFile()
        var recovery = Disk.recover()
        recovery.attachAndLaunch (("a", file, geom), ("b", file2, geom))
        recovery = Disk.recover()
        recovery.reopenAndLaunch ("a") (("a", file), ("b", file2))
      }

      "fail when given extra uninitialized items" in {
        implicit val scheduler = StubScheduler.random()
        val file = StubFile()
        val file2 = StubFile()
        val file3 = StubFile()
        var recovery = Disk.recover()
        recovery.attachAndLaunch (("a", file, geom), ("b", file2, geom))
        recovery = Disk.recover()
        recovery
            .reopenAndWait ("a", "c") (("a", file), ("b", file2), ("c", file3))
            .fail [InconsistentSuperBlocksException]
      }

      "fail when given extra initialized items" in {
        implicit val scheduler = StubScheduler.random()
        val file = StubFile()
        val file2 = StubFile()
        val file3 = StubFile()
        var recovery = Disk.recover()
        recovery.attachAndLaunch (("a", file, geom), ("b", file2, geom))
        recovery = Disk.recover()
        recovery.attachAndLaunch (("c", file3, geom))
        recovery = Disk.recover()
        recovery
            .reopenAndWait ("a", "c") (("a", file), ("b", file2), ("c", file3))
            .fail [ExtraDisksException]
      }}}}
