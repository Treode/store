/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.disk

import java.nio.file.{Path, Paths}
import scala.collection.mutable.UnrolledBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Random

import com.treode.async.Async
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.{CallbackCaptor, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.async.implicits._
import org.scalatest.Assertions

import Assertions.assertResult
import Disk.Launch

private object DiskTestTools {

  type AttachItem = (Path, StubFile, DriveGeometry)

  type ReattachItem = (Path, StubFile)

  val sysid = new Array [Byte] (0)

  implicit def stringToPath (path: String): Path =
    Paths.get (path)

  implicit class RichControllerAgent (controller: Disk.Controller) {
    val agent = controller.asInstanceOf [ControllerAgent]
    import agent.disk

    def assertDisks (paths: String*): Unit =
      disk.assertDisks (paths: _*)

    def assertDraining (paths: String*): Unit =
      disk.assertDraining (paths: _*)

    def attachAndWait (items: AttachItem*): Async [Unit] =
      disk.attachAndWait (items: _*)

    def attachAndCapture (items: AttachItem*): CallbackCaptor [Unit] =
      attachAndWait (items: _*) .capture

    def attachAndPass (items: AttachItem*) (implicit scheduler: StubScheduler) {
      attachAndWait (items: _*) .expectPass()
      disk.assertReady()
    }

    def drainAndWait (items: Path*): Async [Unit] =
      agent.drain (items: _*)

    def drainAndCapture (items: Path*): CallbackCaptor [Unit] =
      drainAndWait (items: _*) .capture()

    def drainAndPass (items: Path*) (implicit scheduler: StubScheduler) {
      drainAndWait (items: _*) .expectPass()
      disk.tickle()
      disk.assertReady()
    }}

  implicit class RichDiskAgent (disk: Disk) {
    val agent = disk.asInstanceOf [DiskAgent]
    import agent.kit.{drives, checkpointer, compactor, config, logd, paged}

    def attachAndWait (items: AttachItem*): Async [Unit] =
      drives._attach (items)

    def assertDisks (paths: String*) {
      val expected = paths.map (Paths.get (_)) .toSet
      val actual = drives.drives.values.map (_.path) .toSet
      assertResult (expected) (actual)
    }

    def assertDraining (paths: String*) {
      val draining = paths.map (Paths.get (_)) .toSet
      assert (drives.drives.values forall (disk => disk.draining == (draining contains disk.path)))
    }

    def assertLaunched() {
      assert (checkpointer.checkpoints != null, "Expected checkpointer to have a registry.")
      assert (compactor.pages != null, "Expected compactor to have a page registry.")
      assertReady()
    }

    def assertReady()  {
      assert (!drives.queue.engaged, "Expected disk to be disengaged.")
      assert (!checkpointer.engaged, "Expected checkpointer to be disengaged.")
      assert (!compactor.engaged, "Expected compactor to be disengaged.")
      val nreceivers = drives.drives.values.filterNot (_.draining) .size
      assertResult (nreceivers) (logd.receivers.size)
      assertResult (nreceivers) (paged.receivers.size)
    }

    def assertInLedger (pos: Position, typ: TypeId, obj: ObjectId, grp: GroupId) (
        implicit scheduler: StubScheduler) {
      val drive = drives.drives (pos.disk)
      val num = (pos.offset >> drive.geom.segmentBits).toInt
      if (num == drive.pageSeg.num) {
        val ledger = drive.pageLedger
        assert (
            ledger.get (typ, obj, grp) > 0,
            s"Expected ($typ, $obj, $grp) in restored from log.")
      } else {
        assert (!drive.alloc.free.contains (num), "Expected segment to be allocated.")
        val seg = drive.geom.segmentBounds (num)
        val ledger = PageLedger.read (drive.file, drive.geom, seg.base) .expectPass()
        assert (
            ledger.get (typ, obj, grp) > 0,
            s"Expected ($typ, $obj, $grp) in ledger at ${seg.base} on ${drive.id}.")
      }}

    // After detaching, closed multiplexers may still reside in the dispatcher's receiver
    // queue.  This tickles each receiver, and only the open ones will reinstall themselves.
    private def tickle [M] (dispatcher: Dispatcher [M]) (
        implicit tag: ClassTag [M], scheduler: StubScheduler): Int = {
      while (!dispatcher.receivers.isEmpty)
        dispatcher.receivers.remove().pass ((0L, new UnrolledBuffer [M]))
      scheduler.run()
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

  implicit class RichDriveGeometryObject (obj: DriveGeometry.type) {

    def test (
        segmentBits: Int = 12,
        blockBits: Int = 6,
        diskBytes: Long = 1<<20
    ) (implicit
        config: Disk.Config
     ): DriveGeometry =
       DriveGeometry (
           segmentBits,
           blockBits,
           diskBytes)
  }

  implicit class RichLaunchAgent (launch: Disk.Launch) {
    val agent = launch.asInstanceOf [LaunchAgent]
    import agent.disk

    def launchAndPass (tickle: Boolean = false) (implicit scheduler: StubScheduler) {
      agent.launch()
      scheduler.run()
      if (tickle)
        disk.tickle()
      disk.assertLaunched()
    }}

  implicit class RichPager [P] (pager: PageDescriptor [P]) {

    def assertInLedger (pos: Position, obj: ObjectId, grp: GroupId) (
        implicit scheduler: StubScheduler, disk: Disk): Unit =
      disk.assertInLedger (pos, pager.id, obj, grp)

    def fetch (pos: Position) (implicit disk: Disk): Async [P] =
      disk.asInstanceOf [DiskAgent] .kit.drives.fetch (pager, pos)
  }

  implicit class RichRandom (random: Random) {

    /** Choose `count` unique integers between 0 inclusive and max exclusive. */
    def nextInts (count: Int, max: Int): Set [Int] = {
      var ks = Set.empty [Int]
      while (ks.size < count)
        ks += random.nextInt (max)
      ks
    }

    def nextGroup(): GroupId = random.nextLong()
  }

  implicit class RichRecovery (recovery: Disk.Recovery) {
    val agent = recovery.asInstanceOf [RecoveryAgent]

    def attachAndWait (items: AttachItem*): Async [Launch] =
      agent._attach (sysid, items: _*)

    def attachAndCapture (items: AttachItem*): CallbackCaptor [Launch] =
      attachAndWait (items: _*) .capture()

    def attachAndControl (items: AttachItem*)  (
        implicit scheduler: StubScheduler): Disk.Controller = {
      val launch = attachAndWait (items: _*) .expectPass()
      launch.launchAndPass()
      launch.controller
    }

    def attachAndLaunch (items: AttachItem*) (implicit scheduler: StubScheduler): Disk = {
      val launch = attachAndWait (items: _*) .expectPass()
      launch.launchAndPass()
      launch.disk
    }

    def reattachAndWait (items: ReattachItem*): Async [Launch] =
      agent._reattach (items: _*)

    def reattachAndLaunch (items: ReattachItem*) (implicit scheduler: StubScheduler): Disk = {
      val launch = reattachAndWait (items: _*) .expectPass()
      launch.launchAndPass()
      launch.disk
    }

    def reopenAndWait (paths: Path*) (items: ReattachItem*) (
        implicit scheduler: StubScheduler, config: Disk.Config): Async [Launch] = {
      val files = items.toMap
      def superbs (path: Path) = SuperBlocks.read (path, files (path))
      agent._reattach (paths) (superbs _)
    }

    def reopenAndLaunch (paths: Path*) (items: ReattachItem*) (
        implicit scheduler: StubScheduler, config: Disk.Config): Disk = {
      val launch = reopenAndWait (paths: _*) (items: _*) .expectPass()
      launch.launchAndPass()
      launch.disk
    }}}
