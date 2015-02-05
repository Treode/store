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

import com.treode.async.Async
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.{StubGlobals, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.disk.stubs.CrashChecks
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.FreeSpec
import org.scalatest.time.SpanSugar

import Async.{latch, supply}
import DiskTestTools._
import SpanSugar._

class DiskSystemSpec extends FreeSpec with CrashChecks {

  Logger.getLogger ("com.treode") .setLevel (Level.WARNING)

  override val timeLimit = 15 minutes

  "The logger should replay items" - {

    "without checkpoints using one disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DiskTestConfig()
        val geom = DriveGeometry.test()
        val tracker = new LogTracker
        var file: StubFile = null

        setup { implicit scheduler =>
          file = StubFile (1<<20, geom.blockBits)
          implicit val recovery = Disk.recover()
          val launch = recovery.attachAndWait (("a", file, geom)) .expectPass()
          import launch.{controller, disk}
          launch.checkpoint (fail ("Expected no checkpoints"))
          launch.launch()
          for {
            _ <- tracker.batches (80, 40, 10)
            _ <- launch.controller.shutdown()
          } yield ()
        }

        .recover { implicit scheduler =>
          file = StubFile (file.data, geom.blockBits)
          implicit val recovery = Disk.recover()
          val replayer = new LogReplayer
          replayer.attach (recovery)
          implicit val disk = recovery.reattachAndLaunch (("a", file))
          replayer.check (tracker)
        }}}

    "using one disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DiskTestConfig (checkpointEntries = 57)
        val geom = DriveGeometry.test()
        val tracker = new LogTracker
        var file: StubFile = null
        var checkpoint = false

        setup { implicit scheduler =>
          file = StubFile (1<<20, geom.blockBits)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.attachAndWait (("a", file, geom)) .expectPass()
          import launch.{controller, disk}
          tracker.attach()
          launch.checkpoint (supply (checkpoint = true))
          launch.launch()
          for {
            _ <- tracker.batches (80, 40, 10)
            _ <- controller.shutdown()
          } yield ()
        }

        .assert (checkpoint, "Expected a checkpoint")

        .recover { implicit scheduler =>
          file = StubFile (file.data, geom.blockBits)
          implicit val recovery = Disk.recover()
          val replayer = new LogReplayer
          replayer.attach (recovery)
          implicit val disk = recovery.reattachAndLaunch (("a", file))
          replayer.check (tracker)
        }}}

    "using multiple disks" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DiskTestConfig (checkpointEntries = 57)
        val geom = DriveGeometry.test()
        val tracker = new LogTracker
        var file1: StubFile = null
        var file2: StubFile = null
        var file3: StubFile = null
        var checkpoint = false

        setup { implicit scheduler =>
          file1 = StubFile (1<<20, geom.blockBits)
          file2 = StubFile (1<<20, geom.blockBits)
          file3 = StubFile (1<<20, geom.blockBits)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.attachAndWait (
              ("a", file1, geom),
              ("b", file2, geom),
              ("c", file3, geom)) .expectPass()
          import launch.{controller, disk}
          tracker.attach()
          launch.checkpoint (supply (checkpoint = true))
          launch.launch()
          for {
            _ <- tracker.batches (80, 40, 10)
            _ <- controller.shutdown()
          } yield ()
        }

        .assert (checkpoint, "Expected a checkpoint")

        .recover { implicit scheduler =>
          file1 = StubFile (file1.data, geom.blockBits)
          file2 = StubFile (file2.data, geom.blockBits)
          file3 = StubFile (file3.data, geom.blockBits)
          implicit val recovery = Disk.recover()
          val replayer = new LogReplayer
          replayer.attach (recovery)
          implicit val disk = recovery.reattachAndLaunch (
              ("a", file1),
              ("b", file2),
              ("c", file3))
          replayer.check (tracker)
        }}}

    "while attaching a disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DiskTestConfig (checkpointEntries = 17)
        val geom = DriveGeometry.test()
        val tracker = new LogTracker
        var file1: StubFile = null
        var file2: StubFile = null
        var checkpoint = false

        setup { implicit scheduler =>
          file1 = StubFile (1<<20, geom.blockBits)
          file2 = StubFile (1<<20, geom.blockBits)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.attachAndWait (("a", file1, geom)) .expectPass()
          import launch.{controller, disk}
          tracker.attach()
          launch.checkpoint (supply (checkpoint = true))
          launch.launch()
          for {
            _ <- tracker.batches (80, 2, 10, 0)
            _ <- latch (
                tracker.batch (80, 2, 10),
                controller.attachAndWait (("b", file2, geom)))
            _ <- tracker.batches (80, 2, 10, 3)
            _ <- controller.shutdown()
          } yield ()
        }

        .assert (checkpoint, "Expected a checkpoint")

        .recover { implicit scheduler =>
          file1 = StubFile (file1.data, geom.blockBits)
          file2 = StubFile (file2.data, geom.blockBits)
          implicit val recovery = Disk.recover()
          val replayer = new LogReplayer
          replayer.attach (recovery)
          implicit val disk = recovery.reopenAndLaunch ("a") (("a", file1), ("b", file2))
          replayer.check (tracker)
        }}}

    "while draining a disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DiskTestConfig (checkpointEntries = 17)
        val geom = DriveGeometry.test()
        val tracker = new LogTracker
        var file1: StubFile = null
        var file2: StubFile = null
        var checkpoint = false

        setup { implicit scheduler =>
          file1 = StubFile (1<<20, geom.blockBits)
          file2 = StubFile (1<<20, geom.blockBits)
          implicit val recovery = Disk.recover()
          implicit val launch =
            recovery.attachAndWait (("a", file1, geom), ("b", file2, geom)) .expectPass()
          import launch.{controller, disk}
          tracker.attach()
          launch.checkpoint (supply (checkpoint = true))
          launch.launch()
          for {
            _ <- tracker.batches (80, 2, 3, 0)
            _ <- latch (
                tracker.batch (80, 2, 3),
                controller.drainAndWait ("b"))
            _ <- tracker.batches (80, 2, 3, 3)
            _ <- controller.shutdown()
          } yield ()
        }

        .assert (checkpoint, "Expected a checkpoint")

        .recover { implicit scheduler =>
          file1 = StubFile (file1.data, geom.blockBits)
          file2 = StubFile (file2.data, geom.blockBits)
          implicit val recovery = Disk.recover()
          val replayer = new LogReplayer
          replayer.attach (recovery)
          implicit val launch = recovery.reopenAndWait ("a") (("a", file1), ("b", file2)) .expectPass()
          import launch.disk
          tracker.attach()
          launch.launchAndPass (tickle = true)
          replayer.check (tracker)
        }}}}

  "The logger should write more data than disk" - {

    "when randomly scheduled" taggedAs (Intensive, Periodic) in {
      forAllRandoms { implicit random =>

        implicit val config = DiskTestConfig (
            maximumRecordBytes = 1<<9,
            maximumPageBytes = 1<<9,
            checkpointEntries = 1000,
            cleaningFrequency = 3)
        val geom = DriveGeometry.test (
            segmentBits = 10,
            blockBits = 6,
            diskBytes = 1<<16)
        val tracker = new LogTracker

        implicit val scheduler = StubScheduler.random (random)
        val file = StubFile (1<<20, geom.blockBits)
        implicit val recovery = Disk.recover()
        implicit val launch = recovery.attachAndWait (("a", file, geom)) .expectPass()
        import launch.{controller, disk}
        tracker.attach()
        launch.launchAndPass()
        tracker.batches (20, 1000, 2) .expectPass()
        controller.shutdown() .expectPass()
      }}

    "when multithreaded" taggedAs (Intensive, Periodic) in {
      import StubGlobals.scheduler

      implicit val config = DiskTestConfig (
          maximumRecordBytes = 1<<9,
          maximumPageBytes = 1<<9,
          checkpointEntries = 1000,
          cleaningFrequency = 3)
      val geom = DriveGeometry.test (
          segmentBits = 10,
          blockBits = 6,
          diskBytes = 1<<16)
      val tracker = new LogTracker

      implicit val random = Random
      val file = StubFile (1<<20, geom.blockBits)
      implicit val recovery = Disk.recover()
      implicit val launch = recovery.attachAndWait (("a", file, geom)) .await()
      import launch.{controller, disk}
      tracker.attach()
      launch.launch()
      tracker.batches (20, 1000, 2) .await()
      controller.shutdown() .await()
    }}

  "The pager should read and write" - {

    "without cleaning using one disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DiskTestConfig()
        val geom = DriveGeometry.test()
        val tracker = new StuffTracker
        var file: StubFile = null

        setup { implicit scheduler =>
          file = StubFile (1<<20, geom.blockBits)
          implicit val recovery = Disk.recover()
          val launch = recovery.attachAndWait (("a", file, geom)) .expectPass()
          import launch.{controller, disk}
          launch.launch()
          for {
            _ <- tracker.batch (40, 10)
            _ <- controller.shutdown()
          } yield ()
        }

        .recover { implicit scheduler =>
          file = StubFile (file.data, geom.blockBits)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.reattachAndWait (("a", file)) .expectPass()
          import launch.disk
          tracker.attach()
          launch.launchAndPass()
          tracker.check()
        }}}

    "using one disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DiskTestConfig (cleaningFrequency = 3)
        val geom = DriveGeometry.test()
        val tracker = new StuffTracker
        var file: StubFile = null

        setup { implicit scheduler =>
          file = StubFile (1<<20, geom.blockBits)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.attachAndWait (("a", file, geom)) .expectPass()
          import launch.{controller, disk}
          tracker.attach()
          launch.launch()
          for {
            _ <- tracker.batch (40, 10)
            _ <- controller.shutdown()
          } yield ()
        }

        .assert (tracker.probed && tracker.compacted, "Expected cleaning.")

        .recover { implicit scheduler =>
          file = StubFile (file.data, geom.blockBits)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.reattachAndWait (("a", file)) .expectPass()
          import launch.disk
          tracker.attach()
          launch.launchAndPass()
          tracker.check()
        }}}

    "using multiple disks" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DiskTestConfig (cleaningFrequency = 3)
        val geom = DriveGeometry.test()
        val tracker = new StuffTracker
        var file1: StubFile = null
        var file2: StubFile = null
        var file3: StubFile = null

        setup { implicit scheduler =>
          file1 = StubFile (1<<20, geom.blockBits)
          file2 = StubFile (1<<20, geom.blockBits)
          file3 = StubFile (1<<20, geom.blockBits)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.attachAndWait (
              ("a", file1, geom),
              ("b", file2, geom),
              ("c", file3, geom)) .expectPass()
          import launch.{controller, disk}
          tracker.attach()
          launch.launch()
          for {
            _ <- tracker.batch (40, 10)
            _ <- controller.shutdown()
          } yield ()
        }

        .assert (tracker.probed && tracker.compacted, "Expected cleaning.")

        .recover { implicit scheduler =>
          file1 = StubFile (file1.data, geom.blockBits)
          file2 = StubFile (file2.data, geom.blockBits)
          file3 = StubFile (file3.data, geom.blockBits)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.reattachAndWait (
              ("a", file1),
              ("b", file2),
              ("c", file3)) .expectPass()
          import launch.disk
          tracker.attach()
          launch.launchAndPass()
          tracker.check()
        }}}

    "while attaching a disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DiskTestConfig (cleaningFrequency = 3)
        val geom = DriveGeometry.test()
        val tracker = new StuffTracker
        var file1: StubFile = null
        var file2: StubFile = null

        setup { implicit scheduler =>
          file1 = StubFile (1<<20, geom.blockBits)
          file2 = StubFile (1<<20, geom.blockBits)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.attachAndWait (("a", file1, geom)) .expectPass()
          import launch.{disk, controller}
          tracker.attach()
          launch.launch()

          for {
            _ <- tracker.batch (7, 10)
            _ <- latch (
                tracker.batch (7, 10),
                controller.attachAndWait (("b", file2, geom)))
            _ <- tracker.batch (7, 10)
            _ <- controller.shutdown()
          } yield ()
        }

        .assert (tracker.probed && tracker.compacted, "Expected cleaning.")

        .recover { implicit scheduler =>
          file1 = StubFile (file1.data, geom.blockBits)
          file2 = StubFile (file2.data, geom.blockBits)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.reopenAndWait ("a") (("a", file1), ("b", file2)) .expectPass()
          import launch.disk
          tracker.attach()
          launch.launchAndPass()
          tracker.check()
        }}}

    "while draining a disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DiskTestConfig (cleaningFrequency = 3)
        val geom = DriveGeometry.test()
        val tracker = new StuffTracker
        var file1: StubFile = null
        var file2: StubFile = null

        setup { implicit scheduler =>
          file1 = StubFile (1<<20, geom.blockBits)
          file2 = StubFile (1<<20, geom.blockBits)
          implicit val recovery = Disk.recover()
          implicit val launch =
            recovery.attachAndWait (("a", file1, geom), ("b", file2, geom)) .expectPass()
          import launch.{disk, controller}
          tracker.attach()
          launch.launch()

          for {
            _ <- tracker.batch (7, 10)
            _ <- latch (
                tracker.batch (7, 10),
                controller.drainAndWait ("b"))
            _ <- tracker.batch (7, 10)
            _ <- controller.shutdown
          } yield ()
        }

        .assert (tracker.probed && tracker.compacted, "Expected cleaning.")

        .recover { implicit scheduler =>
          file1 = StubFile (file1.data, geom.blockBits)
          file2 = StubFile (file2.data, geom.blockBits)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.reopenAndWait ("a") (("a", file1), ("b", file2)) .expectPass()
          import launch.disk
          tracker.attach()
          launch.launchAndPass (tickle = true)
          tracker.check()
        }}}

    "more data than disk" taggedAs (Intensive, Periodic) in {
      forAllRandoms { implicit random =>

        implicit val config = DiskTestConfig (
            maximumRecordBytes = 1<<9,
            maximumPageBytes = 1<<9,
            checkpointEntries = 1000,
            cleaningFrequency = 3)
        val geom = DriveGeometry.test (
            segmentBits = 10,
            blockBits = 6,
            diskBytes = 1<<18)
        val tracker = new StuffTracker

        implicit val scheduler = StubScheduler.random (random)
        val file = StubFile (1<<18, geom.blockBits)
        implicit val recovery = Disk.recover()
        implicit val launch = recovery.attachAndWait (("a", file, geom)) .expectPass()
        import launch.{controller, disk}
        tracker.attach()
        launch.launch()
        tracker.batch (100, 10) .expectPass()
        controller.shutdown() .expectPass()
        assert (tracker.maximum > (1<<17), "Expected growth.")
      }}}}
