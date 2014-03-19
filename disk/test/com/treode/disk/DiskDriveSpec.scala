package com.treode.disk

import java.nio.file.Paths

import com.treode.async.{Async, AsyncConversions, AsyncTestTools, StubScheduler}
import com.treode.async.io.{File, StubFile}
import org.scalatest.FreeSpec

import AsyncConversions._
import AsyncTestTools._

class DiskDriveSpec extends FreeSpec {

  class DistinguishedException extends Exception

  implicit val config = DisksConfig (0, 8, 1<<10, 100, 3, 1)
  val path = Paths.get ("a")
  val geom = DiskGeometry (10, 4, 1<<20)

  private def init (geom: DiskGeometry, file: File, kit: DisksKit) = {
    val free = IntSet()
    val boot = BootBlock (0, 0, 0, Set (path))
    new SuperBlock (0, boot, geom, false, free, 0, 0, 0, 0)
    DiskDrive.init (0, path, file, geom, boot, kit)
  }

  "DiskDrive.init should" - {

    "work when all is well" in {
      implicit val scheduler = StubScheduler.random()
      val file = new StubFile
      val kit = new DisksKit
      val disk = init (geom, file, kit) .pass
    }

    "issued three writes to the disk" in {
      implicit val scheduler = StubScheduler.random()
      val file = new StubFile
      val kit = new DisksKit
      file.stop = true
      val cb = init (geom, file, kit) .capture()
      scheduler.runTasks()
      file.last.pass()
      file.last.pass()
      file.last.pass()
      file.stop = false
      scheduler.runTasks()
      cb.passed
    }

    "fail when it cannot write the superblock" in {
      implicit val scheduler = StubScheduler.random()
      val file = new StubFile
      val kit = new DisksKit
      file.stop = true
      val cb = init (geom, file, kit) .capture()
      scheduler.runTasks()
      file.last.pass()
      file.last.pass()
      file.last.fail (new DistinguishedException)
      file.stop = false
      scheduler.runTasks()
      cb.failed [DistinguishedException]
    }

    "fail when it cannot write the log tail" in {
      implicit val scheduler = StubScheduler.random()
      val file = new StubFile
      val kit = new DisksKit
      file.stop = true
      val cb = init (geom, file, kit) .capture()
      scheduler.runTasks()
      file.last.pass()
      file.last.fail (new DistinguishedException)
      file.last.pass()
      file.stop = false
      scheduler.runTasks()
      cb.failed [DistinguishedException]
    }

    "fail when it cannot write the page ledger" in {
      implicit val scheduler = StubScheduler.random()
      val file = new StubFile
      val kit = new DisksKit
      file.stop = true
      val cb = init (geom, file, kit) .capture()
      scheduler.runTasks()
      file.last.fail (new DistinguishedException)
      file.last.pass()
      file.last.pass()
      file.stop = false
      scheduler.runTasks()
      cb.failed [DistinguishedException]
    }

  }

}
