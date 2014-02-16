package com.treode.disk

import java.nio.file.Paths

import com.treode.async.{Async, AsyncTestTools, StubScheduler}
import com.treode.async.io.File

private object DiskTestTools extends AsyncTestTools {

  implicit class RichRecovery (recovery: Recovery) (implicit scheduler: StubScheduler) {

    type AttachItem = (String, File, DiskGeometry)

    private def _attach (items: Seq [AttachItem]): Async [DiskDrives] = {
      val _items = items map (v => (Paths.get (v._1), v._2, v._3))
      recovery.attach (_items) .map (_.asInstanceOf [DiskDrives])
    }

    def attachAndLaunch (items: (String, File, DiskGeometry)*): DiskDrives = {
      val disks = _attach (items) .pass
      disks.assertLaunched()
      disks
    }

    private def _reattach (items: Seq [(String, File)]): Async [DiskDrives] = {
      val _items = items map (v => (Paths.get (v._1), v._2))
      recovery.reattach (_items) .map (_.asInstanceOf [DiskDrives])
    }

    def reattachAndLaunch (items: (String, File)*): DiskDrives = {
      val disks = _reattach (items) .pass
      disks.assertLaunched()
      disks
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
    }}}
