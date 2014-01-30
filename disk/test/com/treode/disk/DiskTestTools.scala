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

    type AttachItem = (String, File, DiskDriveConfig)

    private def _attachAndPass (items: Seq [AttachItem], launch: Boolean): DiskDrives = {
      val _items = items map (v => (Paths.get (v._1), v._2, v._3))
      val cb = new CallbackCaptor [Disks]
      recovery.attach (_items, cb)
      scheduler.runTasks()
      val disks = cb.passed.asInstanceOf [DiskDrives]
      if (launch)
        disks.assertLaunched (false)
      else
        disks.assertLaunching()
      disks
    }

    def attachHoldLaunch (items: (String, File, DiskDriveConfig)*): DiskDrives =
      _attachAndPass (items, false)

    def attachAndLaunch (items: (String, File, DiskDriveConfig)*): DiskDrives =
      _attachAndPass (items, true)

    def reattachAndPass (items: (String, File)*): DiskDrives = {
      val _items = items map (v => (Paths.get (v._1), v._2))
      val cb = new CallbackCaptor [Disks]
      recovery.reattach (_items, cb)
      scheduler.runTasks()
      cb.passed.asInstanceOf [DiskDrives]
    }}

  implicit class RichDiskDrives (disks: DiskDrives) (implicit scheduler: StubScheduler) {

    def assertLaunching() =
      assert (
          disks.state.isInstanceOf [DiskDrives#Launching],
          s"Expected Ready, found ${disks.state}")

    def assertLaunched (attaching: Boolean) {
      assert (
          disks.state.isInstanceOf [DiskDrives#Launched],
          s"Expected Launched($attaching), found ${disks.state}")
      assert (
          disks.state.asInstanceOf [DiskDrives#Launched].attaching == attaching,
          s"Expected Launched($attaching), found ${disks.state}")
    }

    def assertPanicked() =
      assert (
          disks.state.isInstanceOf [DiskDrives#Panicked],
          s"Expected Panicked, found ${disks.state}")

    def expectDisks (gen: Int) (items: (Int, String)*) {
      expectResult (items.size) (disks.disks.size)
      for ((id, path) <- items) {
        val disk = disks.disks.values.find (_.path == Paths.get (path)) .get
        expectResult (id) (disk.id)
      }}

    def attachAndPass (items: (String, File, DiskDriveConfig)*) {
      val _items = items map (v => (Paths.get (v._1), v._2, v._3))
      val cb = new CallbackCaptor [Unit]
      disks.attach (_items, cb)
      scheduler.runTasks()
      cb.passed
      assertLaunched (false)
    }

    def attachAndFail [E] (items: (String, File, DiskDriveConfig)*) (implicit m: Manifest [E]) {
      val _items = items map (v => (Paths.get (v._1), v._2, v._3))
      val cb = new CallbackCaptor [Unit]
      disks.attach (_items, cb)
      scheduler.runTasks()
      m.runtimeClass.isInstance (cb.failed)
      assertLaunched (false)
    }

    def attachAndHold (items: (String, File, DiskDriveConfig)*): CallbackCaptor [Unit] = {
      val _items = items map (v => (Paths.get (v._1), v._2, v._3))
      val cb = new CallbackCaptor [Unit]
      disks.attach (_items, cb)
      cb
    }

    def checkpointAndPass() {
      val cb = new CallbackCaptor [Unit]
      disks.checkpoint (cb)
      scheduler.runTasks()
      cb.passed
      assertLaunched (false)
    }

    def checkpointAndHold(): CallbackCaptor [Unit] = {
      val cb = new CallbackCaptor [Unit]
      disks.checkpoint (cb)
      cb
    }

    def checkpointAndQueue(): CallbackCaptor [Unit] = {
      val cb = new CallbackCaptor [Unit]
      disks.checkpoint (cb)
      scheduler.runTasks()
      assert (!cb.wasInvoked)
      cb
    }

    def writeAndPass [G, P] (desc: PageDescriptor [G, P], group: G, page: P): Position = {
      val cb = new CallbackCaptor [Position]
      disks.write (desc, group, page, cb)
      scheduler.runTasks()
      cb.passed
    }

    def readAndPass [G, P] (desc: PageDescriptor [G, P], pos: Position): P = {
      val cb = new CallbackCaptor [P]
      disks.read (desc, pos, cb)
      scheduler.runTasks()
      cb.passed
    }}
}
