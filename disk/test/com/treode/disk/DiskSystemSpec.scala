package com.treode.disk

import scala.util.Random

import com.treode.async.Async
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.disk.stubs.CrashChecks
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.FreeSpec
import org.scalatest.time.SpanSugar

import Async.{latch, supply}
import DiskTestTools._
import SpanSugar._

class DiskSystemSpec extends FreeSpec with CrashChecks {

  override val timeLimit = 15 minutes

  "The logger should replay items" - {

    "without checkpoints using one disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DiskTestConfig()
        val geometry = DiskGeometry.test()
        val tracker = new LogTracker
        var file: StubFile = null

        setup { implicit scheduler =>
          file = StubFile (1<<20)
          implicit val recovery = Disk.recover()
          val launch = recovery.attachAndWait (("a", file, geometry)) .pass
          import launch.{controller, disk}
          launch.checkpoint (fail ("Expected no checkpoints"))
          launch.launch()
          for {
            _ <- tracker.batches (80, 40, 10)
            _ <- launch.controller.shutdown()
          } yield ()
        }

        .recover { implicit scheduler =>
          file = StubFile (file.data)
          implicit val recovery = Disk.recover()
          val replayer = new LogReplayer
          replayer.attach (recovery)
          implicit val disk = recovery.reattachAndLaunch (("a", file))
          replayer.check (tracker)
        }}}

    "using one disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DiskTestConfig (checkpointEntries = 57)
        val geometry = DiskGeometry.test()
        val tracker = new LogTracker
        var file: StubFile = null
        var checkpoint = false

        setup { implicit scheduler =>
          file = StubFile (1<<20)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.attachAndWait (("a", file, geometry)) .pass
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
          file = StubFile (file.data)
          implicit val recovery = Disk.recover()
          val replayer = new LogReplayer
          replayer.attach (recovery)
          implicit val disk = recovery.reattachAndLaunch (("a", file))
          replayer.check (tracker)
        }}}

    "using multiple disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DiskTestConfig (checkpointEntries = 57)
        val geometry = DiskGeometry.test()
        val tracker = new LogTracker
        var file1: StubFile = null
        var file2: StubFile = null
        var file3: StubFile = null
        var checkpoint = false

        setup { implicit scheduler =>
          file1 = StubFile (1<<20)
          file2 = StubFile (1<<20)
          file3 = StubFile (1<<20)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.attachAndWait (
              ("a", file1, geometry),
              ("b", file2, geometry),
              ("c", file3, geometry)) .pass
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
          file1 = StubFile (file1.data)
          file2 = StubFile (file2.data)
          file3 = StubFile (file3.data)
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
        val geometry = DiskGeometry.test()
        val tracker = new LogTracker
        var file1: StubFile = null
        var file2: StubFile = null
        var checkpoint = false

        setup { implicit scheduler =>
          file1 = StubFile (1<<20)
          file2 = StubFile (1<<20)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.attachAndWait (("a", file1, geometry)) .pass
          import launch.{controller, disk}
          tracker.attach()
          launch.checkpoint (supply (checkpoint = true))
          launch.launch()
          for {
            _ <- tracker.batches (80, 2, 10, 0)
            _ <- latch (
                tracker.batch (80, 2, 10),
                controller.attachAndWait (("b", file2, geometry)))
            _ <- tracker.batches (80, 2, 10, 3)
            _ <- controller.shutdown()
          } yield ()
        }

        .assert (checkpoint, "Expected a checkpoint")

        .recover { implicit scheduler =>
          file1 = StubFile (file1.data)
          file2 = StubFile (file2.data)
          implicit val recovery = Disk.recover()
          val replayer = new LogReplayer
          replayer.attach (recovery)
          implicit val disk = recovery.reopenAndLaunch ("a") (("a", file1), ("b", file2))
          replayer.check (tracker)
        }}}

    "while draining a disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DiskTestConfig (checkpointEntries = 17)
        val geometry = DiskGeometry.test()
        val tracker = new LogTracker
        var file1: StubFile = null
        var file2: StubFile = null
        var checkpoint = false

        setup { implicit scheduler =>
          file1 = StubFile (1<<20)
          file2 = StubFile (1<<20)
          implicit val recovery = Disk.recover()
          implicit val launch =
            recovery.attachAndWait (("a", file1, geometry), ("b", file2, geometry)) .pass
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
          file1 = StubFile (file1.data)
          file2 = StubFile (file2.data)
          implicit val recovery = Disk.recover()
          val replayer = new LogReplayer
          replayer.attach (recovery)
          implicit val launch = recovery.reopenAndWait ("a") (("a", file1), ("b", file2)) .pass
          import launch.disk
          tracker.attach()
          launch.launchAndPass (tickle = true)
          replayer.check (tracker)
        }}}}

  "The logger should write more data than disk" - {

    "when randomly scheduled" taggedAs (Intensive, Periodic) in {
      forAllSeeds { implicit random =>

        implicit val config = DiskTestConfig (
            maximumRecordBytes = 1<<9,
            maximumPageBytes = 1<<9,
            checkpointEntries = 1000,
            cleaningFrequency = 3)
        val geometry = DiskGeometry.test (
            segmentBits = 10,
            blockBits = 6,
            diskBytes = 1<<16)
        val tracker = new LogTracker

        implicit val scheduler = StubScheduler.random (random)
        val file = StubFile (1<<16)
        implicit val recovery = Disk.recover()
        implicit val launch = recovery.attachAndWait (("a", file, geometry)) .pass
        import launch.{controller, disk}
        tracker.attach()
        launch.launchAndPass()
        tracker.batches (20, 1000, 2) .pass
        controller.shutdown() .pass
      }}

    "when multithreaded" taggedAs (Intensive, Periodic) in {
      multithreaded { implicit scheduler =>

        implicit val config = DiskTestConfig (
            maximumRecordBytes = 1<<9,
            maximumPageBytes = 1<<9,
            checkpointEntries = 1000,
            cleaningFrequency = 3)
        val geometry = DiskGeometry.test (
            segmentBits = 10,
            blockBits = 6,
            diskBytes = 1<<16)
        val tracker = new LogTracker

        implicit val random = Random
        val file = StubFile (1<<20)
        implicit val recovery = Disk.recover()
        implicit val launch = recovery.attachAndWait (("a", file, geometry)) .await()
        import launch.{controller, disk}
        tracker.attach()
        launch.launch()
        tracker.batches (20, 1000, 2) .await()
        controller.shutdown() .await()
      }}}

  "The pager should read and write" - {

    "without cleaning using one disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DiskTestConfig()
        val geometry = DiskGeometry.test()
        val tracker = new StuffTracker
        var file: StubFile = null

        setup { implicit scheduler =>
          file = StubFile (1<<20)
          implicit val recovery = Disk.recover()
          val launch = recovery.attachAndWait (("a", file, geometry)) .pass
          import launch.{controller, disk}
          launch.launch()
          for {
            _ <- tracker.batch (40, 10)
            _ <- controller.shutdown()
          } yield ()
        }

        .recover { implicit scheduler =>
          file = StubFile (file.data)
          implicit val recovery = Disk.recover()
          implicit val disk = recovery.reattachAndLaunch (("a", file))
          tracker.check()
        }}}

    "using one disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DiskTestConfig (cleaningFrequency = 3)
        val geometry = DiskGeometry.test()
        val tracker = new StuffTracker
        var file: StubFile = null

        setup { implicit scheduler =>
          file = StubFile (1<<20)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.attachAndWait (("a", file, geometry)) .pass
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
          file = StubFile (file.data)
          implicit val recovery = Disk.recover()
          implicit val disk = recovery.reattachAndLaunch (("a", file))
          tracker.check()
        }}}

    "using multiple disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DiskTestConfig (cleaningFrequency = 3)
        val geometry = DiskGeometry.test()
        val tracker = new StuffTracker
        var file1: StubFile = null
        var file2: StubFile = null
        var file3: StubFile = null

        setup { implicit scheduler =>
          file1 = StubFile (1<<20)
          file2 = StubFile (1<<20)
          file3 = StubFile (1<<20)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.attachAndWait (
              ("a", file1, geometry),
              ("b", file2, geometry),
              ("c", file3, geometry)) .pass
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
          file1 = StubFile (file1.data)
          file2 = StubFile (file2.data)
          file3 = StubFile (file3.data)
          implicit val recovery = Disk.recover()
          implicit val disk = recovery.reattachAndLaunch (
              ("a", file1),
              ("b", file2),
              ("c", file3))
          tracker.check()
        }}}

    "while attaching a disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DiskTestConfig (cleaningFrequency = 3)
        val geometry = DiskGeometry.test()
        val tracker = new StuffTracker
        var file1: StubFile = null
        var file2: StubFile = null

        setup { implicit scheduler =>
          file1 = StubFile (1<<20)
          file2 = StubFile (1<<20)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.attachAndWait (("a", file1, geometry)) .pass
          import launch.{disk, controller}
          tracker.attach()
          launch.launch()

          for {
            _ <- tracker.batch (7, 10)
            _ <- latch (
                tracker.batch (7, 10),
                controller.attachAndWait (("b", file2, geometry)))
            _ <- tracker.batch (7, 10)
            _ <- controller.shutdown()
          } yield ()
        }

        .assert (tracker.probed && tracker.compacted, "Expected cleaning.")

        .recover { implicit scheduler =>
          file1 = StubFile (file1.data)
          file2 = StubFile (file2.data)
          implicit val recovery = Disk.recover()
          implicit val disk = recovery.reopenAndLaunch ("a") (("a", file1), ("b", file2))
          tracker.check()
        }}}

    "while draining a disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DiskTestConfig (cleaningFrequency = 3)
        val geometry = DiskGeometry.test()
        val tracker = new StuffTracker
        var file1: StubFile = null
        var file2: StubFile = null

        setup { implicit scheduler =>
          file1 = StubFile (1<<20)
          file2 = StubFile (1<<20)
          implicit val recovery = Disk.recover()
          implicit val launch =
            recovery.attachAndWait (("a", file1, geometry), ("b", file2, geometry)) .pass
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
          file1 = StubFile (file1.data)
          file2 = StubFile (file2.data)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.reopenAndWait ("a") (("a", file1), ("b", file2)) .pass
          import launch.disk
          tracker.attach()
          launch.launchAndPass (tickle = true)
          tracker.check()
        }}}

    "more data than disk" taggedAs (Intensive, Periodic) in {
      forAllSeeds { implicit random =>

        implicit val config = DiskTestConfig (
            maximumRecordBytes = 1<<9,
            maximumPageBytes = 1<<9,
            checkpointEntries = 1000,
            cleaningFrequency = 3)
        val geometry = DiskGeometry.test (
            segmentBits = 10,
            blockBits = 6,
            diskBytes = 1<<18)
        val tracker = new StuffTracker

        implicit val scheduler = StubScheduler.random (random)
        val file = StubFile (1<<18)
        implicit val recovery = Disk.recover()
        implicit val launch = recovery.attachAndWait (("a", file, geometry)) .pass
        import launch.{controller, disk}
        tracker.attach()
        launch.launch()
        tracker.batch (100, 10) .pass
        controller.shutdown() .pass
        assert (tracker.maximum > (1<<17), "Expected growth.")
      }}}}
