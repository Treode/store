package com.treode.disk

import java.nio.file.{Path, Paths}
import scala.collection.JavaConversions
import scala.collection.mutable.UnrolledBuffer
import scala.reflect.ClassTag
import scala.util.Random

import com.treode.async.{Async, AsyncTestTools, CallbackCaptor, StubScheduler}
import com.treode.async.io.StubFile
import org.scalatest.Assertions

import Assertions.assertResult
import Disks.Launch
import JavaConversions._

private object DiskTestTools extends AsyncTestTools {

  type AttachItem = (String, StubFile, DiskGeometry)

  type ReattachItem = (String, StubFile)

  implicit class RichControllerAgent (controller: Disks.Controller) {
    val agent = controller.asInstanceOf [ControllerAgent]
    import agent.disks

    def assertDisks (paths: String*): Unit =
      disks.assertDisks (paths: _*)

    def assertDraining (paths: String*): Unit =
      disks.assertDraining (paths: _*)

    def attachAndWait (items: AttachItem*) (implicit scheduler: StubScheduler): Async [Unit] = {
      agent.attach (
        for ((path, file, geom) <- items) yield {
          file.scheduler = scheduler
          (Paths.get (path), file, geom)
        })
    }

    def attachAndCapture (items: AttachItem*) (implicit scheduler: StubScheduler): CallbackCaptor [Unit] =
      attachAndWait (items: _*) .capture

    def attachAndPass (items: AttachItem*) (implicit scheduler: StubScheduler) {
      attachAndWait (items: _*) .pass
      disks.assertReady()
    }

    def drainAndWait (items: String*): Async [Unit] =
      agent.drain (items map (Paths.get (_)))

    def drainAndCapture (items: String*): CallbackCaptor [Unit] =
      drainAndWait (items: _*) .capture()

    def drainAndPass (items: String*) (implicit scheduler: StubScheduler) {
      drainAndWait (items: _*) .pass
      disks.tickle()
      disks.assertReady()
    }

    def checkpoint(): Async [Unit] =
      agent.checkpoint()
  }

  implicit class RichDisksAgent (disks: Disks) {
    val agent = disks.asInstanceOf [DisksAgent]
    import agent.kit.{disks => drives, checkpointer, compactor, logd, paged}

    def assertDisks (paths: String*) {
      val expected = paths.map (Paths.get (_)) .toSet
      val actual = drives.disks.values.map (_.path) .toSet
      assertResult (expected) (actual)
    }

    def assertDraining (paths: String*) {
      val draining = paths.map (Paths.get (_)) .toSet
      assert (drives.disks.values forall (disk => disk.draining == (draining contains disk.path)))
    }

    def assertLaunched() {
      assert (checkpointer.checkpoints != null, "Expected checkpointer to have a registry.")
      assert (compactor.pages != null, "Expected compactor to have a page registry.")
      assertReady()
    }

    def assertReady()  {
      assert (!drives.engaged, "Expected disks to be disengaged.")
      assert (!checkpointer.engaged, "Expected checkpointer to be disengaged.")
      assert (!compactor.engaged, "Expected compactor to be disengaged.")
      assertResult (drives.disks.size) (logd.receivers.size)
      assertResult (drives.disks.size) (paged.receivers.size)
    }

    // After detaching, closed multiplexers may still reside in the dispatcher's receiver
    // queue.  This tickles each receiver, and only the open ones will reinstall themselves.
    private def tickle [M] (dispatcher: Dispatcher [M]) (
        implicit tag: ClassTag [M], scheduler: StubScheduler): Int = {
      while (!dispatcher.receivers.isEmpty)
        dispatcher.receivers.remove () (0L, new UnrolledBuffer [M])
      scheduler.runTasks()
    }

    def tickle() (implicit scheduler: StubScheduler) {
      tickle (logd)
      tickle (paged)
    }

    def checkpoint(): Unit =
      checkpointer.checkpoint()

    def clean() =
      compactor.clean()
  }

  implicit class RichLaunchAgent (launch: Disks.Launch) {
    val agent = launch.asInstanceOf [LaunchAgent]
    import agent.disks

    def launchAndPass (tickle: Boolean = false) (implicit scheduler: StubScheduler) {
      agent.launch()
      scheduler.runTasks()
      if (tickle)
        disks.tickle()
      disks.assertLaunched()
    }}

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
    }}

  implicit class RichRecovery (recovery: Disks.Recovery) {
    val agent = recovery.asInstanceOf [RecoveryAgent]

    def attachAndWait (items: AttachItem*) (implicit scheduler: StubScheduler): Async [Launch] = {
      recovery.attach (
        for ((path, file, geom) <- items) yield {
          file.scheduler = scheduler
          (Paths.get (path), file, geom)
        })
    }

    def attachAndCapture (items: AttachItem*) (implicit scheduler: StubScheduler): CallbackCaptor [Launch] =
      attachAndWait (items: _*) .capture()

    def attachAndControl (items: AttachItem*)  (
        implicit scheduler: StubScheduler): Disks.Controller = {
      val launch = attachAndWait (items: _*) .pass
      launch.launchAndPass()
      launch.controller
    }

    def attachAndLaunch (items: AttachItem*) (implicit scheduler: StubScheduler): Disks = {
      val launch = attachAndWait (items: _*) .pass
      launch.launchAndPass()
      launch.disks
    }

    def reattachAndWait (items: ReattachItem*) (implicit scheduler: StubScheduler): Async [Launch] = {
      recovery.reattach (
        for ((path, file) <- items) yield {
          file.scheduler = scheduler
          (Paths.get (path), file)
        })
    }

    def reattachAndLaunch (items: ReattachItem*) (implicit scheduler: StubScheduler): Disks = {
      val launch = reattachAndWait (items: _*) .pass
      launch.launchAndPass()
      launch.disks
    }

    def reopenAndWait (paths: String*) (items: ReattachItem*) (
        implicit scheduler: StubScheduler, config: DisksConfig): Async [Launch] = {
      val _paths = paths map (Paths.get (_))
      val _items =
        for ((path, file) <- items) yield {
          file.scheduler = scheduler
          (Paths.get (path), file)
        }
      val files = _items.toMap
      def superbs (path: Path) = SuperBlocks.read (path, files (path))
      agent._reattach (_paths) (superbs _)
    }

    def reopenAndLaunch (paths: String*) (items: ReattachItem*) (
        implicit scheduler: StubScheduler, config: DisksConfig): Disks = {
      val launch = reopenAndWait (paths: _*) (items: _*) .pass
      launch.launchAndPass()
      launch.disks
    }}}
