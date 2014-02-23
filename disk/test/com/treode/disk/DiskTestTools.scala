package com.treode.disk

import java.nio.file.Paths

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

    def attachAndLaunch (items: (String, File, DiskGeometry)*): DiskDrives = {
      val launch = _attach (items) .pass
      launch.launch()
      scheduler.runTasks()
      val disks = launch.disks.asInstanceOf [DiskDrives]
      disks.assertLaunched()
      disks
    }

    def attach (item: (String, File, DiskGeometry)): (Launch, DiskDrives) = {
      val launch = _attach (Seq (item)) .pass
      val disks = launch.disks.asInstanceOf [DiskDrives]
      (launch, disks)
    }

    private def _reattach (items: Seq [(String, File)]): Async [Launch] = {
      val _items = items map (v => (Paths.get (v._1), v._2))
      recovery.reattach (_items)
    }

    def reattachAndLaunch (items: (String, File)*): DiskDrives = {
      val launch = _reattach (items) .pass
      launch.launch()
      scheduler.runTasks()
      val disks = launch.disks.asInstanceOf [DiskDrives]
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
