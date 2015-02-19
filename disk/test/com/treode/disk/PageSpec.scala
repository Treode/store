/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.disk

import java.util.logging.{Level, Logger}
import scala.util.Random

import com.treode.async.{Async, Callback}
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.{AsyncCaptor, StubScheduler}
import com.treode.async.stubs.implicits._
import org.scalatest.FreeSpec

import Async.supply
import DiskTestTools._

class PageSpec extends FreeSpec {

  Logger.getLogger ("com.treode") .setLevel (Level.WARNING)

  class DistinguishedException extends Exception

  implicit val config = DiskTestConfig()
  val geom = DriveGeometry.test()

  object pagers {
    import DiskPicklers._
    val str = PageDescriptor (0xE9, string)
    val stuff = PageDescriptor (0x25, Stuff.pickler)
  }

  "The pager should" - {

    def setup (disk: StubFile) (implicit scheduler: StubScheduler) = {
      val recovery = Disk.recover()
      recovery.attachAndLaunch (("a", disk, geom))
    }

    def recover (disk: StubFile) (implicit scheduler: StubScheduler) = {
      val recovery = Disk.recover()
      recovery.reattachAndLaunch (("a", disk))
    }

    "fetch after write and recovery" in {

      var file: StubFile = null
      var pos = Position (0, 0, 0)

      {
        implicit val scheduler = StubScheduler.random()
        file = StubFile (1<<20, geom.blockBits)
        implicit val disk = setup (file)
        pos = pagers.str.write (0, 0, "one") .expectPass()
        pagers.str.fetch (pos) .expectPass ("one")
      }

      {
        implicit val scheduler = StubScheduler.random()
        file = StubFile (file.data, geom.blockBits)
        implicit val disk = recover (file)
        pagers.str.fetch (pos) .expectPass ("one")
      }}

    "read from the cache after write" in {

      var file: StubFile = null
      var pos = Position (0, 0, 0)

      {
        implicit val scheduler = StubScheduler.random()
        file = StubFile (1<<20, geom.blockBits)
        implicit val disk = setup (file)
        pos = pagers.str.write (0, 0, "one") .expectPass()
        file.stop = true
        pagers.str.read (pos) .expectPass ("one")
      }}

    "read from the cache after a first read" in {

      var file: StubFile = null
      var pos = Position (0, 0, 0)

      {
        implicit val scheduler = StubScheduler.random()
        file = StubFile (1<<20, geom.blockBits)
        implicit val disk = setup (file)
        pos = pagers.str.write (0, 0, "one") .expectPass()
      }

      {
        implicit val scheduler = StubScheduler.random()
        file = StubFile (file.data, geom.blockBits)
        implicit val disk = recover (file)
        pagers.str.read (pos) .expectPass ("one")
        file.stop = true
        pagers.str.read (pos) .expectPass ("one")
      }}

    "reject a large page" in {

      {
        implicit val scheduler = StubScheduler.random()
        val file = StubFile (1<<20, geom.blockBits)
        implicit val disk = setup (file)
        pagers.stuff.write (0, 0, Stuff (0, 1000)) .fail [OversizedPageException]
      }}}

  "The compactor should" - {

    "report an unrecognized page" in {

      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val file = StubFile (1<<20, geom.blockBits)
      val recovery = Disk.recover()
      implicit val disk = recovery.attachAndLaunch (("a", file, geom))
      for (i <- 0 until 40)
        pagers.stuff.write (0, 0, Stuff (random.nextLong)) .expectPass()
      disk.clean()
      intercept [IllegalArgumentException] {
        scheduler.run()
      }}

    "report an error from a probe method" in {

      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val file = StubFile (1<<20, geom.blockBits)
      val recovery = Disk.recover()
      implicit val launch = recovery.attachAndWait (("a", file, geom)) .expectPass()
      import launch.disk
      pagers.stuff.handle (new PageHandler {
        def probe (obj: ObjectId, groups: Set [GroupId]): Async [Set [GroupId]] =
          throw new DistinguishedException
        def compact (obj: ObjectId, groups: Set [GroupId]): Async [Unit] =
          throw new AssertionError
      })
      launch.launch()

      for (i <- 0 until 40)
        pagers.stuff.write (0, 0, Stuff (random.nextLong)) .expectPass()
      disk.clean()
      intercept [DistinguishedException] {
        scheduler.run()
      }}

    "report an error from a compact method" in {

      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val file = StubFile (1<<20, geom.blockBits)
      val recovery = Disk.recover()
      implicit val launch = recovery.attachAndWait (("a", file, geom)) .expectPass()
      import launch.disk
      pagers.stuff.handle (new PageHandler {
        def probe (obj: ObjectId, groups: Set [GroupId]): Async [Set [GroupId]] =
          supply (Set (groups.head))
        def compact (obj: ObjectId, groups: Set [GroupId]): Async [Unit] =
          throw new DistinguishedException
      })
      launch.launch()

      for (i <- 0 until 40)
        pagers.stuff.write (0, i, Stuff (random.nextLong)) .expectPass()
      disk.clean()
      intercept [DistinguishedException] {
        scheduler.run()
      }}


    "restart a compaction after a crash" in {

      var file: StubFile = null

      // Write enough pages to disk to allocate a couple segments.
      {
        implicit val config = DiskTestConfig (cleaningFrequency = 3)
        implicit val random = new Random (0)
        implicit val scheduler = StubScheduler.random (random)
        file = StubFile (1<<20, geom.blockBits)
        val recovery = Disk.recover()
        implicit val launch = recovery.attachAndWait (("a", file, geom)) .expectPass()
        import launch.disk
        pagers.stuff.handle (new PageHandler {
          def probe (obj: ObjectId, groups: Set [GroupId]): Async [Set [GroupId]] =
            supply (groups)
          def compact (obj: ObjectId, groups: Set [GroupId]): Async [Unit] =
            supply (())
        })
        launch.launch()

        for (i <- 0 until 20)
          pagers.stuff.write (0, i, Stuff (random.nextLong)) .expectPass()
      }

      // Restart with a low threshold, allocations should trigger a compaction.
      {
        implicit val random = new Random (0)
        implicit val scheduler = StubScheduler.random (random)
        implicit val config = DiskTestConfig (cleaningFrequency = 1)
        val captor = AsyncCaptor [Set [GroupId]]

        file = StubFile (file.data, geom.blockBits)
        val recovery = Disk.recover()
        implicit val launch = recovery.reattachAndWait (("a", file)) .expectPass()
        import launch.disk
        pagers.stuff.handle (new PageHandler {
          def probe (obj: ObjectId, groups: Set [GroupId]): Async [Set [GroupId]] =
            captor.start()
          def compact (obj: ObjectId, groups: Set [GroupId]): Async [Unit] =
            supply (())
        })
        launch.launch()
        scheduler.run()

        assertResult (1) (captor.outstanding)

      }}}}
