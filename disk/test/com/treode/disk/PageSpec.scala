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
import com.treode.async.stubs.StubScheduler
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
    val str = PageDescriptor (0xE9, uint, string)
    val stuff = PageDescriptor (0x25, uint, Stuff.pickler)
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
        pos = pagers.str.write (0, 0, "one") .pass
        pagers.str.fetch (pos) .expect ("one")
      }

      {
        implicit val scheduler = StubScheduler.random()
        file = StubFile (file.data, geom.blockBits)
        implicit val disk = recover (file)
        pagers.str.fetch (pos) .expect ("one")
      }}

    "read from the cache after write" in {

      var file: StubFile = null
      var pos = Position (0, 0, 0)

      {
        implicit val scheduler = StubScheduler.random()
        file = StubFile (1<<20, geom.blockBits)
        implicit val disk = setup (file)
        pos = pagers.str.write (0, 0, "one") .pass
        file.stop = true
        pagers.str.read (pos) .expect ("one")
      }}

    "read from the cache after a first read" in {

      var file: StubFile = null
      var pos = Position (0, 0, 0)

      {
        implicit val scheduler = StubScheduler.random()
        file = StubFile (1<<20, geom.blockBits)
        implicit val disk = setup (file)
        pos = pagers.str.write (0, 0, "one") .pass
      }

      {
        implicit val scheduler = StubScheduler.random()
        file = StubFile (file.data, geom.blockBits)
        implicit val disk = recover (file)
        pagers.str.read (pos) .expect ("one")
        file.stop = true
        pagers.str.read (pos) .expect ("one")
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
        pagers.stuff.write (0, 0, Stuff (random.nextLong)) .pass
      disk.clean()
      intercept [IllegalArgumentException] {
        scheduler.run()
      }}

    "report an error from a probe method" in {

      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val file = StubFile (1<<20, geom.blockBits)
      val recovery = Disk.recover()
      implicit val launch = recovery.attachAndWait (("a", file, geom)) .pass
      import launch.disk
      pagers.stuff.handle (new PageHandler [Int] {
        def probe (obj: ObjectId, groups: Set [Int]): Async [Set [Int]] =
          throw new DistinguishedException
        def compact (obj: ObjectId, groups: Set [Int]): Async [Unit] =
          throw new AssertionError
      })
      launch.launch()

      for (i <- 0 until 40)
        pagers.stuff.write (0, 0, Stuff (random.nextLong)) .pass
      disk.clean()
      intercept [DistinguishedException] {
        scheduler.run()
      }}

    "report an error from a compact method" in {

      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val file = StubFile (1<<20, geom.blockBits)
      val recovery = Disk.recover()
      implicit val launch = recovery.attachAndWait (("a", file, geom)) .pass
      import launch.disk
      pagers.stuff.handle (new PageHandler [Int] {
        def probe (obj: ObjectId, groups: Set [Int]): Async [Set [Int]] =
          supply (Set (groups.head))
        def compact (obj: ObjectId, groups: Set [Int]): Async [Unit] =
          throw new DistinguishedException
      })
      launch.launch()

      for (i <- 0 until 40)
        pagers.stuff.write (0, i, Stuff (random.nextLong)) .pass
      disk.clean()
      intercept [DistinguishedException] {
        scheduler.run()
      }}}}
