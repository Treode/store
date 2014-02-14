package com.treode.disk

import java.nio.file.Paths
import scala.reflect.ClassTag

import com.treode.async.{CallbackCaptor, StubScheduler, callback}
import com.treode.async.io.File
import com.treode.pickle.Pickler
import org.scalatest.Assertions

import Assertions._

private object DiskTestTools {

  implicit class RichRecovery (recovery: Recovery) (implicit scheduler: StubScheduler) {

    type AttachItem = (String, File, DiskGeometry)

    private def _attachAndPass (items: Seq [AttachItem], launch: Boolean): DiskDrives = {
      val _items = items map (v => (Paths.get (v._1), v._2, v._3))
      val disks =
        CallbackCaptor.pass [Disks] (recovery.attach (_items, _)) .asInstanceOf [DiskDrives]
      disks.assertLaunched()
      disks
    }

    def attachAndLaunch (items: (String, File, DiskGeometry)*): DiskDrives =
      _attachAndPass (items, true)

    def reattachAndPass (items: (String, File)*): DiskDrives = {
      val _items = items map (v => (Paths.get (v._1), v._2))
      CallbackCaptor.pass [Disks] (recovery.reattach (_items, _)) .asInstanceOf [DiskDrives]
    }}

  implicit class RichDiskDrives (disks: DiskDrives) (implicit scheduler: StubScheduler) {

    def assertLaunched() {
      assert (!disks.engaged, "Expected disks to be disengaged.")
      val cp = disks.checkpointer
      assert (cp.checkpoints != null, "Expected checkpointer to have a registry.")
      assert (!cp.engaged, "Expected checkpointer to be disengaged.")
      val c = disks.compactor
      assert (c.pages != null, "Expected compactor to have a page registry.")
      assert (!c.engaged, "Expected compactor to be disengaged.")
    }

    def writeAndPass [G, P] (desc: PageDescriptor [G, P], group: G, page: P): Position =
      CallbackCaptor.pass [Position] (disks.write (desc, group, page, _))

    def readAndPass [P] (desc: PageDescriptor [_, P], pos: Position): P =
      CallbackCaptor.pass [P] (disks.read (desc, pos, _))
  }}
