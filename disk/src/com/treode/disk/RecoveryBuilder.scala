package com.treode.disk

import java.nio.file.Path
import java.util.ArrayList
import java.util.concurrent.ExecutorService

import com.treode.async.{Callback, Scheduler}
import com.treode.async.io.File

private class RecoveryBuilder (implicit scheduler: Scheduler) extends Recovery {

  private val records = new RecordRegistry
  private val loaders = new ReloadRegistry
  private val launches = new ArrayList [Launch => Any]
  private var open = true

  def requireOpen(): Unit =
    require (open, "Recovery has already begun.")

  def reload [B] (desc: RootDescriptor [B]) (f: B => Reload => Any) {
    requireOpen()
    loaders.reload (desc) (f)
  }

  def replay [R] (desc: RecordDescriptor [R]) (f: R => Any): Unit = {
    requireOpen()
    records.replay (desc) (f)
  }

  def launch (f: Launch => Any): Unit = synchronized {
    requireOpen()
    launches.add (f)
  }

  def close (cb: Callback [Disks]): RecoveryAgent = {
    open = false
    new RecoveryAgent (records, loaders, launches, cb)
  }

  def reattach (items: Seq [(Path, File)], cb: Callback [Disks]): Unit =
    close (cb) .reattach (items)

  def reattach (items: Seq [Path], exec: ExecutorService, cb: Callback [Disks]): Unit =
    close (cb) .reattach (items, exec)

  def attach (items: Seq [(Path, File, DiskDriveConfig)], cb: Callback [Disks]): Unit =
    close (cb) .attach (items)

  def attach (items: Seq [(Path, DiskDriveConfig)], exec: ExecutorService, cb: Callback [Disks]): Unit =
    close (cb) .attach (items, exec)
}
