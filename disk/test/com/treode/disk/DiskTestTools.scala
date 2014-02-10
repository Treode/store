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
      val disks =
        CallbackCaptor.pass [Disks] (recovery.attach (_items, _)) .asInstanceOf [DiskDrives]
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
      CallbackCaptor.pass [Disks] (recovery.reattach (_items, _)) .asInstanceOf [DiskDrives]
    }}

  implicit class RichDiskDrives (disks: DiskDrives) (implicit scheduler: StubScheduler) {

    def assertLaunching() =
      assert (
          disks.state.isInstanceOf [DiskDrives#Launching],
          s"Expected Ready, found ${disks.state}")

    def assertLaunched (attaching: Boolean) {
      assert (
          disks.state.isInstanceOf [DiskDrives#Launched],
          s"Expected DiskDrives.Launched($attaching), found ${disks.state}")
      assert (
          disks.state.asInstanceOf [DiskDrives#Launched].attaching == attaching,
          s"Expected DiskDrives.Launched($attaching), found ${disks.state}")
    }

    def assertPanicked() =
      assert (
          disks.state.isInstanceOf [DiskDrives#Panicked],
          s"Expected DiskDrives.Panicked, found ${disks.state}")

    def expectDisks (gen: Int) (items: (Int, String)*) {
      expectResult (items.size) (disks.size)
      for ((id, path) <- items) {
        val disk = disks.iterator.find (_.path == Paths.get (path)) .get
        expectResult (id) (disk.id)
      }}

    def attachAndPass (items: (String, File, DiskDriveConfig)*) {
      val _items = items map (v => (Paths.get (v._1), v._2, v._3))
      CallbackCaptor.pass [Unit] (disks.attach (_items, _))
      assertLaunched (false)
    }

    def attachAndFail [E] (items: (String, File, DiskDriveConfig)*) (implicit m: Manifest [E]) {
      val _items = items map (v => (Paths.get (v._1), v._2, v._3))
      CallbackCaptor.fail [E, Unit] (disks.attach (_items, _))
      assertLaunched (false)
    }

    def attachAndHold (items: (String, File, DiskDriveConfig)*): CallbackCaptor [Unit] = {
      val _items = items map (v => (Paths.get (v._1), v._2, v._3))
      val cb = CallbackCaptor [Unit]
      disks.attach (_items, cb)
      cb
    }

    def checkpointAndPass() {
      CallbackCaptor.pass [Unit] (disks.checkpoint _)
      assertLaunched (false)
    }

    def checkpointAndHold(): CallbackCaptor [Unit] = {
      val cb = CallbackCaptor [Unit]
      disks.checkpoint (cb)
      cb
    }

    def checkpointAndQueue(): CallbackCaptor [Unit] = {
      val cb = CallbackCaptor [Unit]
      disks.checkpoint (cb)
      scheduler.runTasks()
      cb.expectNotInvoked
      cb
    }

    def writeAndPass [G, P] (desc: PageDescriptor [G, P], group: G, page: P): Position =
      CallbackCaptor.pass [Position] (disks.write (desc, group, page, _))

    def readAndPass [P] (desc: PageDescriptor [_, P], pos: Position): P =
      CallbackCaptor.pass [P] (disks.read (desc, pos, _))
  }}
