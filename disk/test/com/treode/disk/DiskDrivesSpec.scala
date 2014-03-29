package com.treode.disk

import java.nio.file.Paths

import com.treode.async.StubScheduler
import com.treode.async.io.StubFile
import com.treode.tags.Periodic
import org.scalatest.FreeSpec

import DiskTestTools._

class DiskDrivesSpec extends FreeSpec with CrashChecks {

  implicit val config = TestDisksConfig()
  val geom = TestDiskGeometry()

  "The DiskDrives" - {

    "when ready, should" - {

      "allow attaching a new item" taggedAs (Periodic) in {
        forAllCrashes { implicit random =>

          val file1 = new StubFile () (null)
          val file2 = new StubFile () (null)
          var attached = false

          setup { implicit scheduler =>
            val recovery = Disks.recover()
            attached = false
            for {
              launch <- recovery.attachAndWait (("a", file1, geom))
              controller = launch.controller
              _ <- controller.attachAndWait (("b", file2, geom))
            } yield {
              controller.assertDisks ("a", "b")
              attached = true
            }}

          .recover { implicit scheduler =>
            val recovery = Disks.recover()
            val controller = recovery.reopenAndLaunch ("a") (("a", file1), ("b", file2))
            if (attached)
              controller.assertDisks ("a", "b")
          }}}

      "allow attaching multiple new items" taggedAs (Periodic) in {
        forAllCrashes { implicit random =>

          val file1 = new StubFile () (null)
          val file2 = new StubFile () (null)
          val file3 = new StubFile () (null)
          var attached = false

          setup { implicit scheduler =>
            val recovery = Disks.recover()
            attached = false
            for {
              launch <- recovery.attachAndWait (("a", file1, geom))
              controller = launch.controller
              _ <- controller.attachAndWait (("b", file2, geom), ("c", file3, geom))
            } yield {
              controller.assertDisks ("a", "b", "c")
              attached = true
            }}

          .recover { implicit scheduler =>
            implicit val scheduler = StubScheduler.random()
            val recovery = Disks.recover()
            val controller =
              recovery.reopenAndLaunch ("a") (("a", file1), ("b", file2), ("c", file3))
            if (attached)
              controller.assertDisks ("a", "b", "c")
          }}}

      "reject attaching no items" in {

        val file1 = new StubFile () (null)
        val file2 = new StubFile () (null)

        {
          implicit val scheduler = StubScheduler.random()
          val recovery = Disks.recover()
          val controller = recovery.attachAndControl (("a", file1, geom))
          controller.attachAndWait () .fail [IllegalArgumentException]
          controller.assertDisks ("a")
        }

        {
          implicit val scheduler = StubScheduler.random()
          val recovery = Disks.recover()
          val controller = recovery.reattachAndLaunch (("a", file1))
          controller.assertDisks ("a")
        }}

      "reject attaching the same item multiple times" in {

        val file1 = new StubFile () (null)
        val file2 = new StubFile () (null)

        {
          implicit val scheduler = StubScheduler.random()
          val recovery = Disks.recover()
          val controller = recovery.attachAndControl (("a", file1, geom))
          controller
              .attachAndWait (("b", file2, geom), ("b", file2, geom))
              .fail [IllegalArgumentException]
          controller.assertDisks ("a")
        }

        {
          implicit val scheduler = StubScheduler.random()
          val recovery = Disks.recover()
          val controller = recovery.reattachAndLaunch (("a", file1))
          controller.assertDisks ("a")
        }}

      "reject attaching an item that's already attached" in {

        val file = new StubFile () (null)

        {
          implicit val scheduler = StubScheduler.random()
          val recovery = Disks.recover()
          val controller = recovery.attachAndControl (("a", file, geom))
          controller.attachAndWait (("a", file, geom)).fail [AlreadyAttachedException]
          controller.assertDisks ("a")
        }

        {
          implicit val scheduler = StubScheduler.random()
          val recovery = Disks.recover()
          val controller = recovery.reattachAndLaunch (("a", file))
          controller.assertDisks ("a")
        }}

      "allow draining an item" taggedAs (Periodic) in {
        forAllCrashes { implicit random =>

          val file1 = new StubFile () (null)
          val file2 = new StubFile () (null)
          var drained = false

          setup { implicit scheduler =>
            val recovery = Disks.recover()
            drained = false
            for {
              launch <- recovery.attachAndWait (("a", file1, geom), ("b", file2, geom))
              controller = launch.controller
              _ <- controller.drainAndWait ("b")
            } yield {
              assert (file2.closed)
              controller.assertDisks ("a")
              drained = true
            }}

          .recover { implicit scheduler =>
            val recovery = Disks.recover()
            val controller = recovery.reopenAndLaunch ("a") (("a", file1), ("b", file2))
            if (drained)
              controller.assertDisks ("a")
          }}}

      "allow draining multiple items" taggedAs (Periodic) in {
        forAllCrashes { implicit random =>

          val file1 = new StubFile () (null)
          val file2 = new StubFile () (null)
          val file3 = new StubFile () (null)
          var drained = false

          setup { implicit scheduler =>
            val recovery = Disks.recover()
            drained = false
            for {
              launch <- recovery
                  .attachAndWait (("a", file1, geom), ("b", file2, geom), ("c", file3, geom))
              controller = launch.controller
              _ <- controller.drainAndWait ("b", "c")
            } yield {
              assert (file2.closed)
              assert (file3.closed)
              controller.assertDisks ("a")
              drained = true
            }}

          .recover { implicit scheduler =>
            val recovery = Disks.recover()
            val controller = recovery
              .reopenAndLaunch ("a") (("a", file1), ("b", file2), ("c", file3))
            if (drained)
              controller.assertDisks ("a")
          }}}

      "reject draining no items" in {

        val file = new StubFile () (null)

        {
          implicit val scheduler = StubScheduler.random()
          val recovery = Disks.recover()
          val controller = recovery.attachAndControl (("a", file, geom))
          controller.drainAndWait () .fail [IllegalArgumentException]
          controller.assertDisks ("a")
          controller.assertDraining ()
        }

        {
          implicit val scheduler = StubScheduler.random()
          val recovery = Disks.recover()
          val controller = recovery.reattachAndLaunch (("a", file))
          controller.assertDisks ("a")
        }}

      "reject draining non-existent items" in {

        val file = new StubFile () (null)

        {
          implicit val scheduler = StubScheduler.random()
          val recovery = Disks.recover()
          val controller = recovery.attachAndControl (("a", file, geom))
          controller.drainAndWait ("b") .fail [NotAttachedException]
          controller.assertDisks ("a")
          controller.assertDraining ()
        }

        {
          implicit val scheduler = StubScheduler.random()
          val recovery = Disks.recover()
          val controller = recovery.reattachAndLaunch (("a", file))
          controller.assertDisks ("a")
        }}

      "reject draining all items" in {

        val file = new StubFile () (null)

        {
          implicit val scheduler = StubScheduler.random()
          val recovery = Disks.recover()
          val controller = recovery.attachAndControl (("a", file, geom))
          controller.drainAndWait ("a") .fail [CannotDrainAllException]
          controller.assertDisks ("a")
          controller.assertDraining ()
        }

        {
          implicit val scheduler = StubScheduler.random()
          val recovery = Disks.recover()
          val controller = recovery.reattachAndLaunch (("a", file))
          controller.assertDisks ("a")
        }}}

    "when engaged, should" - {

      "queue attaching a new item" in {

        val file1 = new StubFile () (null)
        val file2 = new StubFile () (null)

        {
          implicit val scheduler = StubScheduler.random()
          val recovery = Disks.recover()
          val launch = recovery.attachAndWait (("a", file1, geom)) .pass
          val controller = launch.controller
          val cb = controller.attachAndCapture (("b", file2, geom))
          cb.assertNotInvoked()
          controller.assertDisks ("a")
          launch.launchAndPass()
          cb.passed
          controller.assertDisks ("a", "b")
        }

        {
          implicit val scheduler = StubScheduler.random()
          val recovery = Disks.recover()
          val controller = recovery.reattachAndLaunch (("a", file1), ("b", file2))
          controller.assertDisks ("a", "b")
        }}

      "pass an exception from a queued attach" in {

        val file = new StubFile () (null)

        {
          implicit val scheduler = StubScheduler.random()
          val recovery = Disks.recover()
          val launch = recovery.attachAndWait (("a", file, geom)) .pass
          val controller = launch.controller
          val cb = controller.attachAndCapture()
          cb.assertNotInvoked()
          controller.assertDisks ("a")
          launch.launchAndPass()
          cb.failed [IllegalArgumentException]
          controller.assertDisks ("a")
        }

        {
          implicit val scheduler = StubScheduler.random()
          val recovery = Disks.recover()
          val controller = recovery.reattachAndLaunch (("a", file))
          controller.assertDisks ("a")
        }}

      "queue draining an item" in {

        val file1 = new StubFile () (null)
        val file2 = new StubFile () (null)

        {
          implicit val scheduler = StubScheduler.random()
          val recovery = Disks.recover()
          val launch = recovery.attachAndWait (("a", file1, geom), ("b", file2, geom)) .pass
          import launch.{controller, disks}
          val cb = controller.drainAndCapture ("b")
          cb.assertNotInvoked()
          controller.assertDisks ("a", "b")
          launch.launchAndPass (tickle = true)
          cb.passed
          assert (file2.closed)
          controller.assertDisks ("a")
        }

        {
          implicit val scheduler = StubScheduler.random()
          val recovery = Disks.recover()
          val controller = recovery.reattachAndLaunch (("a", file1))
          controller.assertDisks ("a")
        }}

      "pass an exception from a queued drain" in {

        val file = new StubFile () (null)

        {
          implicit val scheduler = StubScheduler.random()
          val recovery = Disks.recover()
          val launch = recovery.attachAndWait (("a", file, geom)) .pass
          import launch.{controller, disks}
          val cb = controller.drainAndCapture()
          cb.assertNotInvoked()
          controller.assertDisks ("a")
          launch.launchAndPass (tickle = true)
          cb.failed [IllegalArgumentException]
          controller.assertDisks ("a")
        }

        {
          implicit val scheduler = StubScheduler.random()
          val recovery = Disks.recover()
          val controller = recovery.reattachAndLaunch (("a", file))
          controller.assertDisks ("a")
        }}}}}
