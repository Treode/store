package com.treode.disk

import com.treode.async.Async
import com.treode.async.stubs.implicits._
import com.treode.disk.stubs.{CrashChecks, StubDisks, StubDiskDrive}
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.FreeSpec

import Async.supply
import DiskTestTools._
import DiskSystemSpec._

class StubDisksSpec extends FreeSpec with CrashChecks {

  "The logger should replay items" - {

    "without checkpoints" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        val tracker = new LogTracker
        val drive = new StubDiskDrive

        setup { implicit scheduler =>
          implicit val recovery = StubDisks.recover (0.0)
          val launch = recovery.attach (drive) .pass
          import launch.disks
          launch.checkpoint (fail ("Expected no checkpoints"))
          launch.launch()
          tracker.batches (80, 40, 10)
        }

        .recover { implicit scheduler =>
          implicit val recovery = StubDisks.recover()
          val replayer = new LogReplayer
          replayer.attach (recovery)
          implicit val disks = recovery.reattach (drive) .pass.disks
          replayer.check (tracker)
        }}}

    "with checkpoints" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        val tracker = new LogTracker
        val drive = new StubDiskDrive
        var checkpoint = false

        setup { implicit scheduler =>
          implicit val recovery = StubDisks.recover (0.01)
          val launch = recovery.attach (drive) .pass
          import launch.disks
          launch.checkpoint (supply (checkpoint = true))
          launch.launch()
          tracker.batches (80, 40, 10)
        }

        .assert (checkpoint, "Expected a checkpoint")

        .recover { implicit scheduler =>
          implicit val recovery = StubDisks.recover()
          val replayer = new LogReplayer
          replayer.attach (recovery)
          implicit val disks = recovery.reattach (drive) .pass.disks
          replayer.check (tracker)
        }}}

      "with frequent checkpoints" taggedAs (Intensive, Periodic) in {
        forAllCrashes { implicit random =>

        val tracker = new LogTracker
        val drive = new StubDiskDrive
        var checkpoint = false

        setup { implicit scheduler =>
          implicit val recovery = StubDisks.recover (0.01)
          val launch = recovery.attach (drive) .pass
          import launch.disks
          launch.checkpoint (supply (checkpoint = true))
          launch.launch()
          tracker.batches (80, 40, 10)
        }

        .assert (checkpoint, "Expected a checkpoint")

        .recover { implicit scheduler =>
          implicit val recovery = StubDisks.recover()
          val replayer = new LogReplayer
          replayer.attach (recovery)
          implicit val disks = recovery.reattach (drive) .pass.disks
          replayer.check (tracker)
        }}}}

  "The pager should read and write" - {

    "without cleaning" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        val tracker = new StuffTracker
        val drive = new StubDiskDrive

        setup { implicit scheduler =>
          implicit val recovery = StubDisks.recover (0.0)
          implicit val disks = recovery.attach (drive) .pass.disks
          tracker.batch (40, 10)
        }

        .recover { implicit scheduler =>
          implicit val recovery = StubDisks.recover()
          implicit val disks = recovery.attach (drive) .pass.disks
          tracker.check()
        }}}}}
