package com.treode.disk

import java.nio.file.Paths

import com.treode.async.{Async, AsyncConversions, AsyncTestTools, CallbackCaptor, StubScheduler}
import com.treode.async.io.{File, StubFile}
import com.treode.buffer.PagedBuffer
import com.treode.pickle.Picklers
import org.scalatest.FreeSpec

import AsyncConversions._
import AsyncTestTools._

class DiskDriveSpec extends FreeSpec {

  class DistinguishedException extends Exception

  implicit val config = TestDisksConfig()

  private def init (file: File, kit: DisksKit) = {
    val path = Paths.get ("a")
    val free = IntSet()
    val boot = BootBlock (0, 0, 0, Set (path))
    val geom = TestDiskGeometry()
    new SuperBlock (0, boot, geom, false, free, 0, 0)
    DiskDrive.init (0, path, file, geom, boot, kit)
  }

  "DiskDrive.init should" - {

    "work when all is well" in {
      implicit val scheduler = StubScheduler.random()
      val file = new StubFile
      val kit = new DisksKit (0)
      val drive = init (file, kit) .pass
    }

    "issue two writes to the disk" in {
      implicit val scheduler = StubScheduler.random()
      val file = new StubFile
      val kit = new DisksKit (0)
      file.stop = true
      val cb = init (file, kit) .capture()
      scheduler.runTasks()
      file.last.pass()
      file.last.pass()
      file.stop = false
      scheduler.runTasks()
      cb.passed
    }

    "fail when it cannot write the superblock" in {
      implicit val scheduler = StubScheduler.random()
      val file = new StubFile
      val kit = new DisksKit (0)
      file.stop = true
      val cb = init (file, kit) .capture()
      scheduler.runTasks()
      file.last.pass()
      file.last.fail (new DistinguishedException)
      file.stop = false
      scheduler.runTasks()
      cb.failed [DistinguishedException]
    }

    "fail when it cannot write the log tail" in {
      implicit val scheduler = StubScheduler.random()
      val file = new StubFile
      val kit = new DisksKit (0)
      file.stop = true
      val cb = init (file, kit) .capture()
      scheduler.runTasks()
      file.last.fail (new DistinguishedException)
      file.last.pass()
      file.stop = false
      scheduler.runTasks()
      cb.failed [DistinguishedException]
    }}}
