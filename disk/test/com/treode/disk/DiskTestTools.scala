package com.treode.disk

import java.nio.file.Paths
import scala.util.Random

import com.treode.async.{Async, AsyncTestTools, StubScheduler}
import com.treode.async.io.File

import Disks.Launch

private object DiskTestTools extends AsyncTestTools {

  implicit class RichRecovery (recovery: Disks.Recovery) (implicit scheduler: StubScheduler) {

    type AttachItem = (String, File, DiskGeometry)

    private def _attach (items: Seq [AttachItem]): Async [Launch] = {
      val _items = items map (v => (Paths.get (v._1), v._2, v._3))
      recovery.attach (_items)
    }

    def attachAndLaunch (items: (String, File, DiskGeometry)*): Disks = {
      val launch = _attach (items) .pass.asInstanceOf [LaunchAgent]
      launch.launch()
      scheduler.runTasks()
      launch.kit.assertLaunched()
      launch.disks
    }

    private def _reattach (items: Seq [(String, File)]): Async [Launch] = {
      val _items = items map (v => (Paths.get (v._1), v._2))
      recovery.reattach (_items)
    }

    def reattachAndLaunch (items: (String, File)*): Disks = {
      val launch = _reattach (items) .pass.asInstanceOf [LaunchAgent]
      launch.launch()
      scheduler.runTasks()
      launch.kit.assertLaunched()
      launch.disks
    }}

  implicit class RichDiskKit (kit: DisksKit) (implicit scheduler: StubScheduler) {
    import kit.{disks, checkpointer, compactor}

    def assertLaunched() {
      assert (!disks.engaged, "Expected disks to be disengaged.")
      assert (checkpointer.checkpoints != null, "Expected checkpointer to have a registry.")
      assert (!checkpointer.engaged, "Expected checkpointer to be disengaged.")
      assert (compactor.pages != null, "Expected compactor to have a page registry.")
      assert (!compactor.engaged, "Expected compactor to be disengaged.")
    }}

  implicit class RichRandom (random: Random) {

    /** Choose `count` unique integers between 0 inclusive and max exclusive. */
    def nextInts (count: Int, max: Int): Set [Int] = {
      var ks = Set.empty [Int]
      while (ks.size < count)
        ks += random.nextInt (max)
      ks
    }}}
