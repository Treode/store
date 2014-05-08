package com.treode.disk

import java.nio.file.{Path, Paths}
import scala.collection.JavaConversions
import scala.collection.mutable.UnrolledBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Random

import com.treode.async.Async
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.{CallbackCaptor, StubScheduler}
import com.treode.async.stubs.implicits._
import org.scalatest.Assertions

import Assertions.assertResult
import Disks.Launch
import JavaConversions._

private object DiskTestTools {

  type AttachItem = (Path, StubFile, DiskGeometry)

  type ReattachItem = (Path, StubFile)

  implicit def stringToPath (path: String): Path =
    Paths.get (path)

  implicit class RichControllerAgent (controller: Disks.Controller) {
    val agent = controller.asInstanceOf [ControllerAgent]
    import agent.disks

    def assertDisks (paths: String*): Unit =
      disks.assertDisks (paths: _*)

    def assertDraining (paths: String*): Unit =
      disks.assertDraining (paths: _*)

    def attachAndWait (items: AttachItem*) (implicit scheduler: StubScheduler): Async [Unit] = {
      items foreach (_._2.scheduler = scheduler)
      agent._attach (items: _*)
    }

    def attachAndCapture (items: AttachItem*) (implicit scheduler: StubScheduler): CallbackCaptor [Unit] =
      attachAndWait (items: _*) .capture

    def attachAndPass (items: AttachItem*) (implicit scheduler: StubScheduler) {
      attachAndWait (items: _*) .pass
      disks.assertReady()
    }

    def drainAndWait (items: Path*): Async [Unit] =
      agent.drain (items: _*)

    def drainAndCapture (items: Path*): CallbackCaptor [Unit] =
      drainAndWait (items: _*) .capture()

    def drainAndPass (items: Path*) (implicit scheduler: StubScheduler) {
      drainAndWait (items: _*) .pass
      disks.tickle()
      disks.assertReady()
    }}

  implicit class RichDisksAgent (disks: Disks) {
    val agent = disks.asInstanceOf [DisksAgent]
    import agent.kit.{disks => drives, checkpointer, compactor, config, logd, paged}

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
      assert (!drives.queue.engaged, "Expected disks to be disengaged.")
      assert (!checkpointer.engaged, "Expected checkpointer to be disengaged.")
      assert (!compactor.engaged, "Expected compactor to be disengaged.")
      val nreceivers = drives.disks.values.filterNot (_.draining) .size
      assertResult (nreceivers) (logd.receivers.size)
      assertResult (nreceivers) (paged.receivers.size)
    }

    def assertInLedger (pos: Position, typ: TypeId, obj: ObjectId, grp: PageGroup)
        (implicit scheduler: StubScheduler) {
      val drive = drives.disks (pos.disk)
      val num = (pos.offset >> drive.geometry.segmentBits).toInt
      if (num == drive.pageSeg.num) {
        val ledger = drive.pageLedger
        assert (
            ledger.get (typ, obj, grp) > 0,
            s"Expected ($typ, $obj, $grp) in restored from log.")
      } else {
        assert (!drive.alloc.free.contains (num), "Expected segment to be allocated.")
        val seg = drive.geometry.segmentBounds (num)
        val ledger = PageLedger.read (drive.file, seg.base) .pass
        assert (
            ledger.get (typ, obj, grp) > 0,
            s"Expected ($typ, $obj, $grp) in ledger at ${seg.base}.")
      }}

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

    def checkpoint(): Async [Unit] =
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

    def assertInLedger (pos: Position, obj: ObjectId, grp: G) (
        implicit scheduler: StubScheduler, disks: Disks): Unit =
      disks.assertInLedger (pos, pager.id, obj, PageGroup (pager.pgrp, grp))

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

    def nextGroup(): PageGroup =
      PageGroup (DiskPicklers.fixedLong, random.nextLong())
  }

  implicit class RichRecovery (recovery: Disks.Recovery) {
    val agent = recovery.asInstanceOf [RecoveryAgent]

    def attachAndWait (items: AttachItem*) (implicit scheduler: StubScheduler): Async [Launch] = {
      items foreach (_._2.scheduler = scheduler)
      recovery._attach (items: _*)
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
      items foreach (_._2.scheduler = scheduler)
      recovery._reattach (items: _*)
    }

    def reattachAndLaunch (items: ReattachItem*) (implicit scheduler: StubScheduler): Disks = {
      val launch = reattachAndWait (items: _*) .pass
      launch.launchAndPass()
      launch.disks
    }

    def reopenAndWait (paths: Path*) (items: ReattachItem*) (
        implicit scheduler: StubScheduler, config: DisksConfig): Async [Launch] = {
      items foreach (_._2.scheduler = scheduler)
      val files = items.toMap
      def superbs (path: Path) = SuperBlocks.read (path, files (path))
      agent._reattach (paths) (superbs _)
    }

    def reopenAndLaunch (paths: Path*) (items: ReattachItem*) (
        implicit scheduler: StubScheduler, config: DisksConfig): Disks = {
      val launch = reopenAndWait (paths: _*) (items: _*) .pass
      launch.launchAndPass()
      launch.disks
    }}}
