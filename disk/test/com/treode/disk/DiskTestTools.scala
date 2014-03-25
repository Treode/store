package com.treode.disk

import java.nio.file.Paths
import scala.util.Random

import com.treode.async.{Async, AsyncTestTools, StubScheduler}
import com.treode.async.io.File

import Disks.Launch

private object DiskTestTools extends AsyncTestTools {

  implicit class RichDisksAgent (_agent: Disks) {
    val agent = _agent.asInstanceOf [DisksAgent]
    import agent.kit
    import kit.{disks, checkpointer, compactor}

    def assertLaunched() {
      assert (!disks.engaged, "Expected disks to be disengaged.")
      assert (checkpointer.checkpoints != null, "Expected checkpointer to have a registry.")
      assert (!checkpointer.engaged, "Expected checkpointer to be disengaged.")
      assert (compactor.pages != null, "Expected compactor to have a page registry.")
      assert (!compactor.engaged, "Expected compactor to be disengaged.")
    }

    def checkpoint(): Unit =
      checkpointer.checkpoint()

    def clean() =
      compactor.clean()
  }

  implicit class RichPager [G, P] (pager: PageDescriptor [G, P]) {

    def fetch (pos: Position) (implicit disks: Disks): Async [P] =
      disks.asInstanceOf [DisksAgent] .kit.disks.fetch (pager, pos)
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

    def attachAndCapture (items: AttachItem*): Async [Launch] = {
      val _items = items map (v => (Paths.get (v._1), v._2, v._3))
      recovery.attach (_items)
    }

    def attachAndLaunch (items: AttachItem*): Disks = {
      val launch = attachAndCapture (items: _*) .pass
      import launch.disks
      launch.launch()
      scheduler.runTasks()
      disks.assertLaunched()
      disks
    }

    def reattachAndCapture (items: ReattachItem*): Async [Launch] = {
      val _items = items map (v => (Paths.get (v._1), v._2))
      recovery.reattach (_items)
    }

    def reattachAndLaunch (items: ReattachItem*): Disks = {
      val launch = reattachAndCapture (items: _*) .pass
      import launch.disks
      launch.launch()
      scheduler.runTasks()
      disks.assertLaunched()
      disks
    }}}
