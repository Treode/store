package com.treode.disk

import java.nio.file.Paths
import scala.util.Random

import com.treode.async.{Async, AsyncTestTools, StubScheduler}
import com.treode.async.io.File

import Disks.Launch

private object DiskTestTools extends AsyncTestTools {

  implicit class RichDiskKit (kit: DisksKit) (implicit scheduler: StubScheduler) {
    import kit.{disks, checkpointer, compactor}

    def assertLaunched() {
      assert (!disks.engaged, "Expected disks to be disengaged.")
      assert (checkpointer.checkpoints != null, "Expected checkpointer to have a registry.")
      assert (!checkpointer.engaged, "Expected checkpointer to be disengaged.")
      assert (compactor.pages != null, "Expected compactor to have a page registry.")
      assert (!compactor.engaged, "Expected compactor to be disengaged.")
    }}

  implicit class RichPager [G, P] (pager: PageDescriptor [G, P]) {

    def fetch (pos: Position) (implicit disks: DisksAgent): Async [P] =
      disks.kit.disks.fetch (pager, pos)
  }

  implicit class RichRandom (random: Random) {

    /** Choose `count` unique integers between 0 inclusive and max exclusive. */
    def nextInts (count: Int, max: Int): Set [Int] = {
      var ks = Set.empty [Int]
      while (ks.size < count)
        ks += random.nextInt (max)
      ks
    }

    /** Choose `count` unique longs. */
    def nextSeeds (count: Int): Seq [Long] =
      Seq.fill (count) (random.nextLong)
  }

  implicit class RichRecovery (recovery: Disks.Recovery) (implicit scheduler: StubScheduler) {

    type AttachItem = (String, File, DiskGeometry)
    type ReattachItem = (String, File)

    def attachAndCapture (items: AttachItem*): Async [LaunchAgent] = {
      val _items = items map (v => (Paths.get (v._1), v._2, v._3))
      recovery.attach (_items) .map (_.asInstanceOf [LaunchAgent])
    }

    def attachAndLaunch (items: AttachItem*): DisksAgent = {
      val launch = attachAndCapture (items: _*) .pass
      launch.launch()
      scheduler.runTasks()
      launch.kit.assertLaunched()
      launch.disks.asInstanceOf [DisksAgent]
    }

    def reattachAndCapture (items: ReattachItem*): Async [LaunchAgent] = {
      val _items = items map (v => (Paths.get (v._1), v._2))
      recovery.reattach (_items) .map (_.asInstanceOf [LaunchAgent])
    }

    def reattachAndLaunch (items: ReattachItem*): DisksAgent = {
      val launch = reattachAndCapture (items: _*) .pass
      launch.launch()
      scheduler.runTasks()
      launch.kit.assertLaunched()
      launch.disks.asInstanceOf [DisksAgent]
    }}}
