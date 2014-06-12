package com.treode.disk

import java.nio.file.Paths

import com.treode.async.stubs.StubScheduler
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.implicits._
import com.treode.disk.stubs.CrashChecks
import com.treode.tags.Periodic
import org.scalatest.FreeSpec

import DiskTestTools._

class DiskDrivesSpec extends FreeSpec with CrashChecks {

  implicit val config = DiskTestConfig()
  val geom = DiskGeometry.test()

  "The DiskDrives" - {

    "when ready, should" - {

      "allow attaching a new item" taggedAs (Periodic) in {
        forAllCrashes { implicit random =>

          var file1: StubFile = null
          var file2: StubFile = null
          var attached = false

          setup { implicit scheduler =>
            val recovery = Disk.recover()
            file1 = StubFile()
            file2 = StubFile()
            attached = false
            for {
              launch <- recovery.attachAndWait (("a", file1, geom))
              _ = launch.launch()
              controller = launch.controller
              _ <- controller.attachAndWait (("b", file2, geom))
            } yield {
              controller.assertDisks ("a", "b")
              attached = true
            }}

          .recover { implicit scheduler =>
            file1 = StubFile (file1.data)
            file2 = StubFile (file2.data)
            val recovery = Disk.recover()
            val controller = recovery.reopenAndLaunch ("a") (("a", file1), ("b", file2))
            if (attached)
              controller.assertDisks ("a", "b")
          }}}

      "allow attaching multiple new items" taggedAs (Periodic) in {
        forAllCrashes { implicit random =>

          var file1: StubFile = null
          var file2: StubFile = null
          var file3: StubFile = null
          var attached = false

          setup { implicit scheduler =>
            val recovery = Disk.recover()
            file1 = StubFile()
            file2 = StubFile()
            file3 = StubFile()
            attached = false
            for {
              launch <- recovery.attachAndWait (("a", file1, geom))
              _ = launch.launch()
              controller = launch.controller
              _ <- controller.attachAndWait (("b", file2, geom), ("c", file3, geom))
            } yield {
              controller.assertDisks ("a", "b", "c")
              attached = true
            }}

          .recover { implicit scheduler =>
            file1 = StubFile (file1.data)
            file2 = StubFile (file2.data)
            file3 = StubFile (file3.data)
            val recovery = Disk.recover()
            val controller =
              recovery.reopenAndLaunch ("a") (("a", file1), ("b", file2), ("c", file3))
            if (attached)
              controller.assertDisks ("a", "b", "c")
          }}}

      "reject attaching no items" in {

        var file1: StubFile = null
        var file2: StubFile = null

        {
          implicit val scheduler = StubScheduler.random()
          file1 = StubFile()
          file2 = StubFile()
          val recovery = Disk.recover()
          val controller = recovery.attachAndControl (("a", file1, geom))
          controller.attachAndWait () .fail [IllegalArgumentException]
          controller.assertDisks ("a")
        }

        {
          implicit val scheduler = StubScheduler.random()
          file1 = StubFile (file1.data)
          file2 = StubFile (file2.data)
          val recovery = Disk.recover()
          val controller = recovery.reattachAndLaunch (("a", file1))
          controller.assertDisks ("a")
        }}

      "reject attaching the same item multiple times" in {

        var file1: StubFile = null
        var file2: StubFile = null

        {
          implicit val scheduler = StubScheduler.random()
          file1 = StubFile()
          file2 = StubFile()
          val recovery = Disk.recover()
          val controller = recovery.attachAndControl (("a", file1, geom))
          controller
              .attachAndWait (("b", file2, geom), ("b", file2, geom))
              .fail [IllegalArgumentException]
          controller.assertDisks ("a")
        }

        {
          implicit val scheduler = StubScheduler.random()
          file1 = StubFile (file1.data)
          file2 = StubFile (file2.data)
          val recovery = Disk.recover()
          val controller = recovery.reattachAndLaunch (("a", file1))
          controller.assertDisks ("a")
        }}

      "reject attaching an item that's already attached" in {

        var file: StubFile = null

        {
          implicit val scheduler = StubScheduler.random()
          file = StubFile()
          val recovery = Disk.recover()
          val controller = recovery.attachAndControl (("a", file, geom))
          controller.attachAndWait (("a", file, geom)).fail [ControllerException]
          controller.assertDisks ("a")
        }

        {
          implicit val scheduler = StubScheduler.random()
          file = StubFile (file.data)
          val recovery = Disk.recover()
          val controller = recovery.reattachAndLaunch (("a", file))
          controller.assertDisks ("a")
        }}

      "allow draining an item" taggedAs (Periodic) in {
        forAllCrashes { implicit random =>

          var file1: StubFile = null
          var file2: StubFile = null

          setup { implicit scheduler =>
            val recovery = Disk.recover()
            file1 = StubFile()
            file2 = StubFile()
            for {
              launch <- recovery.attachAndWait (("a", file1, geom), ("b", file2, geom))
              _ = launch.launch()
              controller = launch.controller
              _ <- controller.drainAndWait ("b")
            } yield ()
          }

          .assert (file2.closed, "Expected file to be closed.")

          .recover { implicit scheduler =>
            val detached = file2.closed
            val recovery = Disk.recover()
            file1 = StubFile (file1.data)
            file2 = StubFile (file2.data)
            val controller = recovery.reopenAndLaunch ("a") (("a", file1), ("b", file2))
            if (detached)
              controller.assertDisks ("a")
          }}}

      "allow draining multiple items" taggedAs (Periodic) in {
        forAllCrashes { implicit random =>

          var file1: StubFile = null
          var file2: StubFile = null
          var file3: StubFile = null

          setup { implicit scheduler =>
            val recovery = Disk.recover()
            file1 = StubFile()
            file2 = StubFile()
            file3 = StubFile()
            for {
              launch <-
                  recovery.attachAndWait (("a", file1, geom), ("b", file2, geom), ("c", file3, geom))
              _ = launch.launch()
              controller = launch.controller
              _ <- controller.drainAndWait ("b", "c")
            } yield ()
          }

          .assert (file2.closed && file3.closed, "Expected files to be closed.")

          .recover { implicit scheduler =>
            val detached = file2.closed && file3.closed
            val recovery = Disk.recover()
            file1 = StubFile (file1.data)
            file2 = StubFile (file2.data)
            file3 = StubFile (file3.data)
            val controller = recovery
              .reopenAndLaunch ("a") (("a", file1), ("b", file2), ("c", file3))
            if (detached)
              controller.assertDisks ("a")
          }}}

      "reject draining no items" in {

        var file: StubFile = null

        {
          implicit val scheduler = StubScheduler.random()
          file = StubFile()
          val recovery = Disk.recover()
          val controller = recovery.attachAndControl (("a", file, geom))
          controller.drainAndWait () .fail [IllegalArgumentException]
          controller.assertDisks ("a")
          controller.assertDraining ()
        }

        {
          implicit val scheduler = StubScheduler.random()
          file = StubFile (file.data)
          val recovery = Disk.recover()
          val controller = recovery.reattachAndLaunch (("a", file))
          controller.assertDisks ("a")
        }}

      "reject draining non-existent items" in {

        var file: StubFile = null

        {
          implicit val scheduler = StubScheduler.random()
          file = StubFile()
          val recovery = Disk.recover()
          val controller = recovery.attachAndControl (("a", file, geom))
          controller.drainAndWait ("b") .fail [ControllerException]
          controller.assertDisks ("a")
          controller.assertDraining ()
        }

        {
          implicit val scheduler = StubScheduler.random()
          file = StubFile (file.data)
          val recovery = Disk.recover()
          val controller = recovery.reattachAndLaunch (("a", file))
          controller.assertDisks ("a")
        }}

      "reject draining all items" in {

        var file: StubFile = null

        {
          implicit val scheduler = StubScheduler.random()
          file = StubFile()
          val recovery = Disk.recover()
          val controller = recovery.attachAndControl (("a", file, geom))
          controller.drainAndWait ("a") .fail [ControllerException]
          controller.assertDisks ("a")
          controller.assertDraining ()
        }

        {
          implicit val scheduler = StubScheduler.random()
          file = StubFile (file.data)
          val recovery = Disk.recover()
          val controller = recovery.reattachAndLaunch (("a", file))
          controller.assertDisks ("a")
        }}}

    "when engaged, should" - {

      "queue attaching a new item" in {

        var file1: StubFile = null
        var file2: StubFile = null

        {
          implicit val scheduler = StubScheduler.random()
          file1 = StubFile()
          file2 = StubFile()
          val recovery = Disk.recover()
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
          file1 = StubFile (file1.data)
          file2 = StubFile (file2.data)
          val recovery = Disk.recover()
          val controller = recovery.reattachAndLaunch (("a", file1), ("b", file2))
          controller.assertDisks ("a", "b")
        }}

      "pass an exception from a queued attach" in {

        var file: StubFile = null

        {
          implicit val scheduler = StubScheduler.random()
          file = StubFile()
          val recovery = Disk.recover()
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
          file = StubFile (file.data)
          val recovery = Disk.recover()
          val controller = recovery.reattachAndLaunch (("a", file))
          controller.assertDisks ("a")
        }}

      "queue draining an item" in {

        var file1: StubFile = null
        var file2: StubFile = null

        {
          implicit val scheduler = StubScheduler.random()
          file1 = StubFile()
          file2 = StubFile()
          val recovery = Disk.recover()
          val launch = recovery.attachAndWait (("a", file1, geom), ("b", file2, geom)) .pass
          import launch.{controller, disk}
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
          file1 = StubFile (file1.data)
          file2 = StubFile (file2.data)
          val recovery = Disk.recover()
          val controller = recovery.reattachAndLaunch (("a", file1))
          controller.assertDisks ("a")
        }}

      "pass an exception from a queued drain" in {

        var file: StubFile = null

        {
          implicit val scheduler = StubScheduler.random()
          file = StubFile()
          val recovery = Disk.recover()
          val launch = recovery.attachAndWait (("a", file, geom)) .pass
          import launch.{controller, disk}
          val cb = controller.drainAndCapture()
          cb.assertNotInvoked()
          controller.assertDisks ("a")
          launch.launchAndPass (tickle = true)
          cb.failed [IllegalArgumentException]
          controller.assertDisks ("a")
        }

        {
          implicit val scheduler = StubScheduler.random()
          file = StubFile (file.data)
          val recovery = Disk.recover()
          val controller = recovery.reattachAndLaunch (("a", file))
          controller.assertDisks ("a")
        }}}

     "when shutdown, should" - {

      "queue attaching a new item" in {

        var file1: StubFile = null
        var file2: StubFile = null

        {
          implicit val scheduler = StubScheduler.random()
          file1 = StubFile()
          file2 = StubFile()
          val recovery = Disk.recover()
          val controller = recovery.attachAndControl (("a", file1, geom))
          controller.shutdown() .pass
          val cb = controller.attachAndCapture (("b", file2, geom))
          cb.assertNotInvoked()
          controller.assertDisks ("a")
        }

        {
          implicit val scheduler = StubScheduler.random()
          file1 = StubFile (file1.data)
          file2 = StubFile (file2.data)
          val recovery = Disk.recover()
          val controller = recovery.reattachAndLaunch (("a", file1))
          controller.assertDisks ("a")
        }}

      "queue draining an item" in {

        var file1: StubFile = null
        var file2: StubFile = null

        {
          implicit val scheduler = StubScheduler.random()
          file1 = StubFile()
          file2 = StubFile()
          val recovery = Disk.recover()
          val controller = recovery.attachAndControl (("a", file1, geom), ("b", file2, geom))
          controller.shutdown() .pass
          val cb = controller.drainAndCapture ("b")
          cb.assertNotInvoked()
          controller.assertDisks ("a", "b")
        }

        {
          implicit val scheduler = StubScheduler.random()
          file1 = StubFile (file1.data)
          file2 = StubFile (file2.data)
          val recovery = Disk.recover()
          val controller = recovery.reattachAndLaunch (("a", file1), ("b", file2))
          controller.assertDisks ("a", "b")
        }}}}}
