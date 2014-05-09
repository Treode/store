package com.treode.disk

import scala.util.Random

import com.treode.async.{Async, Callback}
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
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

      var file: StubFile = null
      var pos = Position (0, 0, 0)

      {
        implicit val scheduler = StubScheduler.random()
        file = StubFile()
        implicit val disks = setup (file)
        pos = pagers.str.write (0, 0, "one") .pass
        pagers.str.fetch (pos) .expect ("one")
      }

      {
        implicit val scheduler = StubScheduler.random()
        file = StubFile (file.data)
        implicit val disks = recover (file)
        pagers.str.fetch (pos) .expect ("one")
      }}

    "read from the cache after write" in {

      var file: StubFile = null
      var pos = Position (0, 0, 0)

      {
        implicit val scheduler = StubScheduler.random()
        file = StubFile()
        implicit val disks = setup (file)
        pos = pagers.str.write (0, 0, "one") .pass
        file.stop = true
        pagers.str.read (pos) .expect ("one")
      }}

    "read from the cache after a first read" in {

      var file: StubFile = null
      var pos = Position (0, 0, 0)

      {
        implicit val scheduler = StubScheduler.random()
        file = StubFile()
        implicit val disks = setup (file)
        pos = pagers.str.write (0, 0, "one") .pass
      }

      {
        implicit val scheduler = StubScheduler.random()
        file = StubFile (file.data)
        implicit val disks = recover (file)
        pagers.str.read (pos) .expect ("one")
        file.stop = true
        pagers.str.read (pos) .expect ("one")
      }}

    "reject a large page" in {

      {
        implicit val scheduler = StubScheduler.random()
        val file = StubFile()
        implicit val disks = setup (file)
        pagers.stuff.write (0, 0, Stuff (0, 1000)) .fail [OversizedPageException]
      }}}

  "The compactor should" - {

    "report an unrecognized page" in {

      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val file = StubFile()
      val recovery = Disks.recover()
      implicit val disks = recovery.attachAndLaunch (("a", file, geometry))
      for (i <- 0 until 40)
        pagers.stuff.write (0, 0, Stuff (random.nextLong)) .pass
      disks.clean()
      intercept [IllegalArgumentException] {
        scheduler.runTasks()
      }}

    "report an error from a probe method" in {

      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val file = StubFile()
      val recovery = Disks.recover()
      implicit val launch = recovery.attachAndWait (("a", file, geometry)) .pass
      import launch.disks
      pagers.stuff.handle (new PageHandler [Int] {
        def probe (obj: ObjectId, groups: Set [Int]): Async [Set [Int]] =
          throw new DistinguishedException
        def compact (obj: ObjectId, groups: Set [Int]): Async [Unit] =
          throw new AssertionError
      })
      launch.launch()

      for (i <- 0 until 40)
        pagers.stuff.write (0, 0, Stuff (random.nextLong)) .pass
      disks.clean()
      intercept [DistinguishedException] {
        scheduler.runTasks()
      }}

    "report an error from a compact method" in {

      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val file = StubFile()
      val recovery = Disks.recover()
      implicit val launch = recovery.attachAndWait (("a", file, geometry)) .pass
      import launch.disks
      pagers.stuff.handle (new PageHandler [Int] {
        def probe (obj: ObjectId, groups: Set [Int]): Async [Set [Int]] =
          supply (Set (groups.head))
        def compact (obj: ObjectId, groups: Set [Int]): Async [Unit] =
          throw new DistinguishedException
      })
      launch.launch()

      for (i <- 0 until 40)
        pagers.stuff.write (0, i, Stuff (random.nextLong)) .pass
      disks.clean()
      intercept [DistinguishedException] {
        scheduler.runTasks()
      }}}}
