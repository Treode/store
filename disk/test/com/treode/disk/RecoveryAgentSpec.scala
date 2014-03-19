package com.treode.disk

import java.nio.file.Paths

import com.treode.async.{AsyncConversions, AsyncTestTools, StubScheduler}
import com.treode.async.io.StubFile
import org.scalatest.FreeSpec

import AsyncConversions._
import AsyncTestTools._

class RecoveryAgentSpec extends FreeSpec {

  implicit val config = DisksConfig (0, 8, 1<<10, 100, 3, 1)
  val path = Paths.get ("a")
  val geom = DiskGeometry (10, 4, 1<<20)
  val record = RecordDescriptor (0x1BF6DBABE6A70060L, DiskPicklers.int)

  "RecoveryAgent.replay should" - {

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
      recovery.attach (Seq ((path, file, geom))) .pass
      intercept [IllegalArgumentException] {
        recovery.replay (record) (_ => ())
      }}

    "reject registration of a record descriptor after reattach" in {
      implicit val scheduler = StubScheduler.random()
      val file = new StubFile
      var recovery = Disks.recover()
      recovery.attach (Seq ((path, file, geom))) .pass
      recovery = Disks.recover()
      recovery.reattach (Seq ((path, file))) .pass
      intercept [IllegalArgumentException] {
        recovery.replay (record) (_ => ())
      }}}

  "RecoveryAgent.attach should" - {

    "allow attaching an item" in {
      implicit val scheduler = StubScheduler.random()
      val file = new StubFile
      val recovery = Disks.recover()
      recovery.attach (Seq ((path, file, geom))) .pass
    }

    "reject attaching no items" in {
      implicit val scheduler = StubScheduler.random()
      val recovery = Disks.recover()
      recovery.attach (Seq.empty) .fail [IllegalArgumentException]
    }

    "pass through an exception from DiskDrive.init" in {
      implicit val scheduler = StubScheduler.random()
      val file = new StubFile
      val recovery = Disks.recover()
      file.stop = true
      val cb = recovery.attach (Seq ((path, file, geom))) .capture()
      scheduler.runTasks()
      file.stop = false
      while (file.hasLast)
        file.last.fail (new Exception)
      scheduler.runTasks()
      cb.failed [Exception]
    }}

  "RecoveryAgent.reattach" - {

    "in general should" - {

      "allow reattaching an item" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        var recovery = Disks.recover()
        recovery.attach (Seq ((path, file, geom))) .pass
        recovery = Disks.recover()
        recovery.reattach (Seq ((path, file))) .pass
      }

      "reject reattaching no items" in {
        implicit val scheduler = StubScheduler.random()
        val recovery = Disks.recover()
        recovery.reattach (Seq.empty) .fail [IllegalArgumentException]
      }

      "pass through an exception from chooseSuperBlock" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        val recovery = Disks.recover()
        recovery.reattach (Seq ((path, file))) .fail [NoSuperBlocksException]
      }

      "pass through an exception from verifyReattachment" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        var recovery = Disks.recover()
        recovery.attach (Seq ((path, file, geom))) .pass
        val config2 = DisksConfig (1, 8, 1<<10, 100, 3, 1)
        recovery = Disks.recover () (scheduler, config2)
        recovery.reattach (Seq ((path, file))) .fail [CellMismatchException]
      }}

    "when given opened files should" - {

      "require the given file paths match the boot blocks disks" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        val path2 = Paths.get ("b")
        val file2 = new StubFile
        var recovery = Disks.recover()
        recovery.attach (Seq ((path, file, geom), (path2, file2, geom))) .pass
        recovery = Disks.recover()
        recovery.reattach (Seq ((path, file))) .fail [MissingDisksException]
      }}

    "when given unopened paths should" - {

      "pass when all of the items" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        val path2 = Paths.get ("b")
        val file2 = new StubFile
        var recovery = Disks.recover() .asInstanceOf [RecoveryAgent]
        recovery.attach (Seq ((path, file, geom), (path2, file2, geom))) .pass
        recovery = Disks.recover() .asInstanceOf [RecoveryAgent]
        recovery._reattach (Seq (path, path2)) {
          case `path` => SuperBlocks.read (path, file)
          case `path2` => SuperBlocks.read (path2, file2)
        } .pass
      }

      "pass when given a subset of the items" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        val path2 = Paths.get ("b")
        val file2 = new StubFile
        var recovery = Disks.recover() .asInstanceOf [RecoveryAgent]
        recovery.attach (Seq ((path, file, geom), (path2, file2, geom))) .pass
        recovery = Disks.recover() .asInstanceOf [RecoveryAgent]
        recovery._reattach (Seq (path)) {
          case `path` => SuperBlocks.read (path, file)
          case `path2` => SuperBlocks.read (path2, file2)
        } .pass
      }

      "fail when given extra uninitialized items" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        val path2 = Paths.get ("b")
        val file2 = new StubFile
        val path3 = Paths.get ("v")
        val file3 = new StubFile
        var recovery = Disks.recover() .asInstanceOf [RecoveryAgent]
        recovery.attach (Seq ((path, file, geom), (path2, file2, geom))) .pass
        recovery = Disks.recover() .asInstanceOf [RecoveryAgent]
        recovery._reattach (Seq (path, path3)) {
          case `path` => SuperBlocks.read (path, file)
          case `path2` => SuperBlocks.read (path2, file2)
          case `path3` => SuperBlocks.read (path3, file3)
        } .fail [InconsistentSuperBlocksException]
      }

      "fail when given extra initialized items" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        val path2 = Paths.get ("b")
        val file2 = new StubFile
        val path3 = Paths.get ("v")
        val file3 = new StubFile
        var recovery = Disks.recover() .asInstanceOf [RecoveryAgent]
        recovery.attach (Seq ((path, file, geom), (path2, file2, geom))) .pass
        recovery = Disks.recover() .asInstanceOf [RecoveryAgent]
        recovery.attach (Seq ((path3, file3, geom))) .pass
        recovery = Disks.recover() .asInstanceOf [RecoveryAgent]
        recovery._reattach (Seq (path, path3)) {
          case `path` => SuperBlocks.read (path, file)
          case `path2` => SuperBlocks.read (path2, file2)
          case `path3` => SuperBlocks.read (path3, file3)
        } .fail [ExtraDisksException]
      }}}}
