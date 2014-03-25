package com.treode.disk

import java.nio.file.Paths

import com.treode.async.StubScheduler
import com.treode.async.io.StubFile
import org.scalatest.FreeSpec

import DiskTestTools._

class DiskDrivesSpec extends FreeSpec {

  implicit val config = DisksConfig (0, 8, 1<<10, 100, 3, 1)
  val geom = DiskGeometry (10, 4, 1<<20)

  "The DiskDrives" - {

    "when ready, should" - {

      "allow attaching a new item" in {
        implicit val scheduler = StubScheduler.random()
        val file1 = new StubFile
        val file2 = new StubFile
        val recovery = Disks.recover()
        val controller = recovery.attachAndControl (("a", file1, geom))
        controller.assertDisks ("a")
        controller.attachAndPass (("b", file2, geom))
        controller.assertDisks ("a", "b")
      }

      "allow attaching multiple new items" in {
        implicit val scheduler = StubScheduler.random()
        val file1 = new StubFile
        val file2 = new StubFile
        val file3 = new StubFile
        val recovery = Disks.recover()
        val controller = recovery.attachAndControl (("a", file1, geom))
        controller.attachAndPass (("b", file2, geom), ("c", file3, geom))
        controller.assertDisks ("a", "b", "c")
      }

      "reject attaching no items" in {
        implicit val scheduler = StubScheduler.random()
        val file1 = new StubFile
        val file2 = new StubFile
        val recovery = Disks.recover()
        val controller = recovery.attachAndControl (("a", file1, geom))
        controller.attachAndWait () .fail [IllegalArgumentException]
        controller.assertDisks ("a")
      }

      "reject attaching the same item multiple times" in {
        implicit val scheduler = StubScheduler.random()
        val file1 = new StubFile
        val file2 = new StubFile
        val recovery = Disks.recover()
        val controller = recovery.attachAndControl (("a", file1, geom))
        controller
            .attachAndWait (("b", file2, geom), ("b", file2, geom))
            .fail [IllegalArgumentException]
        controller.assertDisks ("a")
      }

      "reject attaching an item that's already attached" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        val recovery = Disks.recover()
        val controller = recovery.attachAndControl (("a", file, geom))
        controller.attachAndWait (("a", file, geom)).fail [AlreadyAttachedException]
        controller.assertDisks ("a")
      }

      "allow draining an item" in {
        implicit val scheduler = StubScheduler.random()
        val file1 = new StubFile
        val file2 = new StubFile
        val recovery = Disks.recover()
        val controller = recovery.attachAndControl (("a", file1, geom), ("b", file2, geom))
        controller.assertDisks ("a", "b")
        controller.drainAndPass ("b")
        assert (file2.closed)
        controller.assertDisks ("a")
      }

      "allow draining multiple items" in {
        implicit val scheduler = StubScheduler.random()
        val file1 = new StubFile
        val file2 = new StubFile
        val file3 = new StubFile
        val recovery = Disks.recover()
        val controller = recovery.attachAndControl (
            ("a", file1, geom),
            ("b", file2, geom),
            ("c", file3, geom))
        controller.assertDisks ("a", "b", "c")
        controller.drainAndPass ("b", "c")
        assert (file2.closed)
        assert (file3.closed)
        controller.assertDisks ("a")
      }

      "reject draining no items" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        val recovery = Disks.recover()
        val controller = recovery.attachAndControl (("a", file, geom))
        controller.drainAndWait () .fail [IllegalArgumentException]
        controller.assertDisks ("a")
        controller.assertDraining ()
      }

      "reject draining non-existent items" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        val recovery = Disks.recover()
        val controller = recovery.attachAndControl (("a", file, geom))
        controller.drainAndWait ("b") .fail [NotAttachedException]
        controller.assertDisks ("a")
        controller.assertDraining ()
      }

      "reject draining all items" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        val recovery = Disks.recover()
        val controller = recovery.attachAndControl (("a", file, geom))
        controller.drainAndWait ("a") .fail [CannotDrainAllException]
        controller.assertDisks ("a")
        controller.assertDraining ()
      }

      "allow a checkpoint" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        val recovery = Disks.recover()
        val controller = recovery.attachAndControl (("a", file, geom))
        controller.checkpoint() .pass
      }

      "reject a checkpoint when one is already waiting" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
        val recovery = Disks.recover()
        val controller = recovery.attachAndControl (("a", file, geom))
        val cb1 = controller.checkpoint() .capture()
        val cb2 = controller.checkpoint() .capture()
        controller.checkpoint() .fail [IllegalArgumentException]
        cb1.passed
        cb2.passed
      }}

    "when engaged, should" - {

      "queue attaching a new item" in {
        implicit val scheduler = StubScheduler.random()
        val file1 = new StubFile
        val file2 = new StubFile
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

      "pass an exception from a queued attach" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
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

      "queue draining an item" in {
        implicit val scheduler = StubScheduler.random()
        val file1 = new StubFile
        val file2 = new StubFile
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

      "pass an exception from a queued drain" in {
        implicit val scheduler = StubScheduler.random()
        val file = new StubFile
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

      "queue a checkpoint" in {
        implicit val scheduler = StubScheduler.random()
        val file1 = new StubFile
        val file2 = new StubFile
        val recovery = Disks.recover()
        val launch = recovery.attachAndWait (("a", file1, geom)) .pass
        val controller = launch.controller
        val cb = controller.checkpoint() .capture()
        launch.launchAndPass()
        cb.passed
      }

      "reject a checkpoint when one is already waiting" in {
        implicit val scheduler = StubScheduler.random()
        val file1 = new StubFile
        val file2 = new StubFile
        val recovery = Disks.recover()
        val launch = recovery.attachAndWait (("a", file1, geom)) .pass
        val controller = launch.controller
        val cb = controller.checkpoint() .capture()
        controller.checkpoint() .fail [IllegalArgumentException]
        launch.launchAndPass()
        cb.passed
      }}}}
