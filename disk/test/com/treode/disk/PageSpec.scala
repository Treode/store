package com.treode.disk

import scala.util.Random

import com.treode.async.{Async, Callback, StubScheduler}
import com.treode.async.io.StubFile
import org.scalatest.FreeSpec

import Async.supply
import DiskTestTools._

class PageSpec extends FreeSpec {

  class DistinguishedException extends Exception

  implicit val config = TestDisksConfig()
  val geometry = TestDiskGeometry()

  object pagers {
    import DiskPicklers._
    val str = PageDescriptor (0xE9, uint, string)
    val stuff = PageDescriptor (0x25, uint, Stuff.pickler)
  }

  "The pager should" - {

    def setup (disk: StubFile) (implicit scheduler: StubScheduler) = {
      val recovery = Disks.recover()
      recovery.attachAndLaunch (("a", disk, geometry))
    }

    def recover (disk: StubFile) (implicit scheduler: StubScheduler) = {
      val recovery = Disks.recover()
      recovery.reattachAndLaunch (("a", disk))
    }

    "fetch after write and recovery" in {

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

    "read from the cache after write" in {

      val disk = new StubFile () (null)
      var pos = Position (0, 0, 0)

      {
        implicit val scheduler = StubScheduler.random()
        implicit val disks = setup (disk)
        pos = pagers.str.write (0, 0, "one") .pass
        disk.stop = true
        pagers.str.read (pos) .expect ("one")
      }}

    "read from the cache after a first read" in {

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
      }}}


  "The compactor should" - {

    "report an unrecognized page" in {

      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val disk = new StubFile
      val recovery = Disks.recover()
      implicit val disks = recovery.attachAndLaunch (("a", disk, geometry))

      for (i <- 0 until 10)
        pagers.stuff.write (0, 0, Stuff (random.nextLong)) .pass
      disks.clean()
      intercept [IllegalArgumentException] {
        scheduler.runTasks()
      }}

    "report an error from a probe method" in {

      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val disk = new StubFile
      val recovery = Disks.recover()
      implicit val launch = recovery.attachAndWait (("a", disk, geometry)) .pass
      import launch.disks
      pagers.stuff.handle (new PageHandler [Int] {
        def probe (obj: ObjectId, groups: Set [Int]): Async [Set [Int]] =
          throw new DistinguishedException
        def compact (obj: ObjectId, groups: Set [Int]): Async [Unit] =
          throw new AssertionError
      })
      launch.launch()

      for (i <- 0 until 10)
        pagers.stuff.write (0, 0, Stuff (random.nextLong)) .pass
      disks.clean()
      intercept [DistinguishedException] {
        scheduler.runTasks()
      }}

    "report an error from a compact method" in {

      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val disk = new StubFile
      val recovery = Disks.recover()
      implicit val launch = recovery.attachAndWait (("a", disk, geometry)) .pass
      import launch.disks
      pagers.stuff.handle (new PageHandler [Int] {
        def probe (obj: ObjectId, groups: Set [Int]): Async [Set [Int]] =
          supply (Set (groups.head))
        def compact (obj: ObjectId, groups: Set [Int]): Async [Unit] =
          throw new DistinguishedException
      })
      launch.launch()

      for (i <- 0 until 10)
        pagers.stuff.write (0, i, Stuff (random.nextLong)) .pass
      disks.clean()
      intercept [DistinguishedException] {
        scheduler.runTasks()
      }}}}
