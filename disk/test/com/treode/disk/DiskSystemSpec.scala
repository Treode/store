package com.treode.disk

import scala.util.Random

import com.treode.async.Async
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.disk.stubs.CrashChecks
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.FreeSpec

import Async.{latch, supply}
import DiskTestTools._

class DiskSystemSpec extends FreeSpec with CrashChecks {

  "The logger should replay items" - {

    "without checkpoints using one disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = TestDisksConfig()
        val geometry = TestDiskGeometry()
        val tracker = new LogTracker
        var file: StubFile = null

        setup { implicit scheduler =>
          file = StubFile (1<<20)
          implicit val recovery = Disk.recover()
          val launch = recovery.attachAndWait (("a", file, geometry)) .pass
          import launch.disks
          launch.checkpoint (fail ("Expected no checkpoints"))
          launch.launch()
          tracker.batches (80, 40, 10)
        }

        .recover { implicit scheduler =>
          file = StubFile (file.data)
          implicit val recovery = Disk.recover()
          val replayer = new LogReplayer
          replayer.attach (recovery)
          implicit val disks = recovery.reattachAndLaunch (("a", file))
          replayer.check (tracker)
        }}}

    "using one disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = TestDisksConfig (checkpointEntries = 57)
        val geometry = TestDiskGeometry()
        val tracker = new LogTracker
        var file: StubFile = null
        var checkpoint = false

        setup { implicit scheduler =>
          file = StubFile (1<<20)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.attachAndWait (("a", file, geometry)) .pass
          import launch.disks
          tracker.attach()
          launch.checkpoint (supply (checkpoint = true))
          launch.launch()
          tracker.batches (80, 40, 10)
        }

        .assert (checkpoint, "Expected a checkpoint")

        .recover { implicit scheduler =>
          file = StubFile (file.data)
          implicit val recovery = Disk.recover()
          val replayer = new LogReplayer
          replayer.attach (recovery)
          implicit val disks = recovery.reattachAndLaunch (("a", file))
          replayer.check (tracker)
        }}}

    "using multiple disks" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = TestDisksConfig (checkpointEntries = 57)
        val geometry = TestDiskGeometry()
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
          import launch.disks
          tracker.attach()
          launch.checkpoint (supply (checkpoint = true))
          launch.launch()
          tracker.batches (80, 40, 10)
        }

        .assert (checkpoint, "Expected a checkpoint")

        .recover { implicit scheduler =>
          file1 = StubFile (file1.data)
          file2 = StubFile (file2.data)
          file3 = StubFile (file3.data)
          implicit val recovery = Disk.recover()
          val replayer = new LogReplayer
          replayer.attach (recovery)
          implicit val disks = recovery.reattachAndLaunch (
              ("a", file1),
              ("b", file2),
              ("c", file3))
          replayer.check (tracker)
        }}}

    "while attaching a disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = TestDisksConfig (checkpointEntries = 17)
        val geometry = TestDiskGeometry()
        val tracker = new LogTracker
        var file1: StubFile = null
        var file2: StubFile = null
        var checkpoint = false

        setup { implicit scheduler =>
          file1 = StubFile (1<<20)
          file2 = StubFile (1<<20)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.attachAndWait (("a", file1, geometry)) .pass
          import launch.{disks, controller}
          tracker.attach()
          launch.checkpoint (supply (checkpoint = true))
          launch.launch()
          for {
            _ <- tracker.batches (80, 2, 10, 0)
            _ <- latch (
                tracker.batch (80, 2, 10),
                controller.attachAndWait (("b", file2, geometry)))
            _ <- tracker.batches (80, 2, 10, 3)
          } yield ()
        }

        .assert (checkpoint, "Expected a checkpoint")

        .recover { implicit scheduler =>
          file1 = StubFile (file1.data)
          file2 = StubFile (file2.data)
          implicit val recovery = Disk.recover()
          val replayer = new LogReplayer
          replayer.attach (recovery)
          implicit val disks = recovery.reopenAndLaunch ("a") (("a", file1), ("b", file2))
          replayer.check (tracker)
        }}}

    "while draining a disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = TestDisksConfig (checkpointEntries = 17)
        val geometry = TestDiskGeometry()
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
          import launch.{disks, controller}
          tracker.attach()
          launch.checkpoint (supply (checkpoint = true))
          launch.launch()
          for {
            _ <- tracker.batches (80, 2, 3, 0)
            _ <- latch (
                tracker.batch (80, 2, 3),
                controller.drainAndWait ("b"))
            _ <- tracker.batches (80, 2, 3, 3)
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
          import launch.disks
          tracker.attach()
          launch.launchAndPass (tickle = true)
          replayer.check (tracker)
        }}}}

  "The logger should write more data than disk" - {

    "write more data than disk" taggedAs (Intensive, Periodic) in {
      forAllSeeds { implicit random =>

        implicit val config = TestDisksConfig (checkpointEntries = 1000, cleaningFrequency = 3)
        val geometry = TestDiskGeometry()
        val tracker = new LogTracker

        implicit val scheduler = StubScheduler.random (random)
        val file = StubFile (1<<20)
        implicit val recovery = Disk.recover()
        implicit val launch = recovery.attachAndWait (("a", file, geometry)) .pass
        import launch.disks
        tracker.attach()
        launch.launchAndPass()
        tracker.batches (80, 100000, 10) .pass
      }}

    "when multithreaded" taggedAs (Intensive, Periodic) in {
      multithreaded { implicit scheduler =>

        implicit val config = TestDisksConfig (checkpointEntries = 1000, cleaningFrequency = 3)
        val geometry = TestDiskGeometry()
        val tracker = new LogTracker

        implicit val random = Random
        val file = StubFile (1<<20)
        implicit val recovery = Disk.recover()
        implicit val launch = recovery.attachAndWait (("a", file, geometry)) .await()
        import launch.disks
        tracker.attach()
        launch.launch()
        tracker.batches (80, 100000, 10) .await()
      }}}

  "The pager should read and write" - {

    "without cleaning using one disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = TestDisksConfig()
        val geometry = TestDiskGeometry()
        val tracker = new StuffTracker
        var file: StubFile = null

        setup { implicit scheduler =>
          file = StubFile (1<<20)
          implicit val recovery = Disk.recover()
          implicit val disks = recovery.attachAndLaunch (("a", file, geometry))
          tracker.batch (40, 10)
        }

        .recover { implicit scheduler =>
          file = StubFile (file.data)
          implicit val recovery = Disk.recover()
          implicit val disks = recovery.reattachAndLaunch (("a", file))
          tracker.check()
        }}}

    "using one disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = TestDisksConfig (cleaningFrequency = 3)
        val geometry = TestDiskGeometry()
        val tracker = new StuffTracker
        var file: StubFile = null

        setup { implicit scheduler =>
          file = StubFile (1<<20)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.attachAndWait (("a", file, geometry)) .pass
          import launch.disks
          tracker.attach()
          launch.launch()
          tracker.batch (40, 10)
        }

        .assert (tracker.probed && tracker.compacted, "Expected cleaning.")

        .recover { implicit scheduler =>
          file = StubFile (file.data)
          implicit val recovery = Disk.recover()
          implicit val disks = recovery.reattachAndLaunch (("a", file))
          tracker.check()
        }}}

    "using multiple disks" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = TestDisksConfig (cleaningFrequency = 3)
        val geometry = TestDiskGeometry()
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
          import launch.disks
          tracker.attach()
          launch.launch()
          tracker.batch (40, 10)
        }

        .assert (tracker.probed && tracker.compacted, "Expected cleaning.")

        .recover { implicit scheduler =>
          file1 = StubFile (file1.data)
          file2 = StubFile (file2.data)
          file3 = StubFile (file3.data)
          implicit val recovery = Disk.recover()
          implicit val disks = recovery.reattachAndLaunch (
              ("a", file1),
              ("b", file2),
              ("c", file3))
          tracker.check()
        }}}

    "while attaching a disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = TestDisksConfig (cleaningFrequency = 3)
        val geometry = TestDiskGeometry()
        val tracker = new StuffTracker
        var file1: StubFile = null
        var file2: StubFile = null

        setup { implicit scheduler =>
          file1 = StubFile (1<<20)
          file2 = StubFile (1<<20)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.attachAndWait (("a", file1, geometry)) .pass
          import launch.{disks, controller}
          tracker.attach()
          launch.launch()

          for {
            _ <- tracker.batch (7, 10)
            _ <- latch (
                tracker.batch (7, 10),
                controller.attachAndWait (("b", file2, geometry)))
            _ <- tracker.batch (7, 10)
          } yield ()
        }

        .assert (tracker.probed && tracker.compacted, "Expected cleaning.")

        .recover { implicit scheduler =>
          file1 = StubFile (file1.data)
          file2 = StubFile (file2.data)
          implicit val recovery = Disk.recover()
          implicit val disks = recovery.reopenAndLaunch ("a") (("a", file1), ("b", file2))
          tracker.check()
        }}}

    "while draining a disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = TestDisksConfig (cleaningFrequency = 3)
        val geometry = TestDiskGeometry()
        val tracker = new StuffTracker
        var file1: StubFile = null
        var file2: StubFile = null

        setup { implicit scheduler =>
          file1 = StubFile (1<<20)
          file2 = StubFile (1<<20)
          implicit val recovery = Disk.recover()
          implicit val launch =
            recovery.attachAndWait (("a", file1, geometry), ("b", file2, geometry)) .pass
          import launch.{disks, controller}
          tracker.attach()
          launch.launch()

          for {
            _ <- tracker.batch (7, 10)
            _ <- latch (
                tracker.batch (7, 10),
                controller.drainAndWait ("b"))
            _ <- tracker.batch (7, 10)
          } yield ()
        }

        .assert (tracker.probed && tracker.compacted, "Expected cleaning.")

        .recover { implicit scheduler =>
          file1 = StubFile (file1.data)
          file2 = StubFile (file2.data)
          implicit val recovery = Disk.recover()
          implicit val launch = recovery.reopenAndWait ("a") (("a", file1), ("b", file2)) .pass
          import launch.disks
          tracker.attach()
          launch.launchAndPass (tickle = true)
          tracker.check()
        }}}

    "more data than disk" taggedAs (Intensive, Periodic) in {
      forAllSeeds { implicit random =>

        implicit val config = TestDisksConfig (cleaningFrequency = 3)
        val geometry = TestDiskGeometry (diskBytes=1<<22)
        val tracker = new StuffTracker

        implicit val scheduler = StubScheduler.random (random)
        val file = StubFile (1<<20)
        implicit val recovery = Disk.recover()
        implicit val launch = recovery.attachAndWait (("a", file, geometry)) .pass
        import launch.disks
        tracker.attach()
        launch.launch()
        tracker.batch (1000, 10) .pass
        assert (tracker.maximum > (1<<21), "Expected growth.")
      }}}}
