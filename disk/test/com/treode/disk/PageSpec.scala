package com.treode.disk

import scala.util.Random

import com.treode.async.{Callback, StubScheduler}
import com.treode.async.io.StubFile
import org.scalatest.FlatSpec

import DiskTestTools._

class PageSpec extends FlatSpec {

  implicit val config = DisksConfig (0, 8, 1<<30, 1<<30, 1<<30, 1)
  val geometry = DiskGeometry (10, 6, 1<<20)

  object pagers {
    import DiskPicklers._
    val str = PageDescriptor (0xE9, uint, string)
    val stuff = PageDescriptor (0x25, uint, Stuff.pickler)
  }

  private def setup (disk: StubFile) (implicit scheduler: StubScheduler) = {
    disk.scheduler = scheduler
    val recovery = Disks.recover()
    recovery.attachAndLaunch (("a", disk, geometry))
  }

  private def recover (disk: StubFile) (implicit scheduler: StubScheduler) = {
    disk.scheduler = scheduler
    val recovery = Disks.recover()
    recovery.reattachAndLaunch (("a", disk))
  }

  "The pager" should "fetch after write and recovery" in {

    val disk = new StubFile () (null)
    var pos = Position (0, 0, 0)

    {
      implicit val scheduler = StubScheduler.random()
      implicit val disks = setup (disk)
      pos = pagers.str.write (0, 0, "one") .pass
      pagers.str.fetch (pos) .expect ("one")
    }

    {
      implicit val scheduler = StubScheduler.random()
      implicit val disks = recover (disk)
      pagers.str.fetch (pos) .expect ("one")
    }}

  it should "read from the cache after write" in {

    val disk = new StubFile () (null)
    var pos = Position (0, 0, 0)

    {
      implicit val scheduler = StubScheduler.random()
      implicit val disks = setup (disk)
      pos = pagers.str.write (0, 0, "one") .pass
      disk.stop = true
      pagers.str.read (pos) .expect ("one")
    }}

  it should "read from the cache after a first read" in {

    val disk = new StubFile () (null)
    var pos = Position (0, 0, 0)

    {
      implicit val scheduler = StubScheduler.random()
      implicit val disks = setup (disk)
      pos = pagers.str.write (0, 0, "one") .pass
    }

    {
      implicit val scheduler = StubScheduler.random()
      implicit val disks = recover (disk)
      pagers.str.read (pos) .expect ("one")
      disk.stop = true
      pagers.str.read (pos) .expect ("one")
    }}

  "The compactor" should "work" in {
    implicit val random = new Random (0)
    implicit val scheduler = StubScheduler.random (random)
    val disk = new StubFile
    val recovery = Disks.recover()
    implicit val disks = recovery.attachAndLaunch (("a", disk, geometry))
    for (i <- 0 until 10)
      pagers.stuff.write (0, 0, Stuff (random.nextLong)) .pass
    disks.clean()
    scheduler.runTasks()
  }
}
