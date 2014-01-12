package com.treode.store.disk2

import java.nio.file.Paths
import scala.reflect.ClassTag

import com.treode.async.{CallbackCaptor, StubScheduler}
import com.treode.async.io.File
import com.treode.cluster.events.StubEvents
import com.treode.pickle.Pickler
import org.scalatest.Assertions

import Assertions._

private class RichDisksKit (scheduler: StubScheduler)
extends DisksKit (scheduler, StubEvents) {

  def assertOpening() = assert (state.isInstanceOf [Opening])
  def assertReady() = assert (state == Ready)
  def assertPanicked() = assert (state.isInstanceOf [Panicked])

  def expectDisks (gen: Int) (items: (Int, String)*) {
    expectResult (items.size) (disks.size)
    for ((id, path) <- items) {
      val disk = disks.values.find (_.path == Paths.get (path)) .get
      expectResult (id) (disk.id)
    }}

  def attachAndPass (items: (String, File, DiskDriveConfig)*) {
    val _items = items map (v => (Paths.get (v._1), v._2, v._3))
    val cb = new CallbackCaptor [Unit]
    attach (_items, cb)
    scheduler.runTasks()
    cb.passed
    assertReady()
  }

  def attachAndFail [E] (items: (String, File, DiskDriveConfig)*) (implicit m: Manifest [E]) {
    val _items = items map (v => (Paths.get (v._1), v._2, v._3))
    val cb = new CallbackCaptor [Unit]
    attach (_items, cb)
    scheduler.runTasks()
    m.runtimeClass.isInstance (cb.failed)
    if (disks.size == 0)
      assertOpening()
    else
      assertReady()
  }

  def attachAndHold (items: (String, File, DiskDriveConfig)*): CallbackCaptor [Unit] = {
    val _items = items map (v => (Paths.get (v._1), v._2, v._3))
    val cb = new CallbackCaptor [Unit]
    attach (_items, cb)
    cb
  }

  def reattachAndPass (items: (String, File)*) {
    val _items = items map (v => (Paths.get (v._1), v._2))
    val cb = new CallbackCaptor [Unit]
    reattach (_items, cb)
    scheduler.runTasks()
    cb.passed
    assertReady()
  }

  def reattachAndFail [E] (items: (String, File)*) (implicit m: Manifest [E]) {
    val _items = items map (v => (Paths.get (v._1), v._2))
    val cb = new CallbackCaptor [Unit]
    reattach (_items, cb)
    scheduler.runTasks()
    m.runtimeClass.isInstance (cb.failed)
    if (m.runtimeClass.isAssignableFrom (classOf [RecoveryCompletedException]))
      assertReady()
    else
      assertPanicked()
  }

  def reattachAndHold (items: (String, File)*): CallbackCaptor [Unit] = {
    val _items = items map (v => (Paths.get (v._1), v._2))
    val cb = new CallbackCaptor [Unit]
    reattach (_items, cb)
    cb
  }

  def checkpointAndPass() {
    val cb = new CallbackCaptor [Unit]
    checkpoint (cb)
    scheduler.runTasks()
    cb.passed
  }

  def checkpointAndHold(): CallbackCaptor [Unit] = {
    val cb = new CallbackCaptor [Unit]
    checkpoint (cb)
    cb
  }

  def checkpointAndQueue(): CallbackCaptor [Unit] = {
    val cb = new CallbackCaptor [Unit]
    checkpoint (cb)
    scheduler.runTasks()
    assert (!cb.wasInvoked)
    cb
  }

  def writeAndPass [P] (pickle: Pickler [P], page: P): (Int, Long, Int) = {
    val cb = new CallbackCaptor [(Int, Long, Int)]
    write (pickle, page, cb)
    scheduler.runTasks()
    cb.passed
  }

  def readAndPass [P] (pickle: Pickler [P], disk: Int, pos: Long, len: Int) (implicit tag: ClassTag [P]): P = {
    val cb = new CallbackCaptor [P]
    read (pickle, disk, pos, len, cb)
    scheduler.runTasks()
    cb.passed
  }}
