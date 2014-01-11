package com.treode.store.disk2

import java.nio.file.Path
import java.util.concurrent.ExecutorService

import com.google.common.annotations.VisibleForTesting
import com.treode.async.{Callback, Scheduler}
import com.treode.async.io.File

trait Disks {

  def reattach (items: Seq [(Path, File)], cb: Callback [Unit])
  def reattach (items: Seq [Path], executor: ExecutorService, cb: Callback [Unit])
  def attach (items: Seq [(Path, File, DiskDriveConfig)], cb: Callback [Unit])
  def attach (items: Seq [(Path, DiskDriveConfig)], exec: ExecutorService, cb: Callback [Unit])
  def record (entry: LogEntry.Body, cb: Callback [Unit])
}

object Disks {

  def apply (scheduler: Scheduler): Disks =
    new DisksKit (scheduler)
}

