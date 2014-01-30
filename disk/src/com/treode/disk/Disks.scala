package com.treode.disk

import java.nio.file.Path
import java.util.concurrent.ExecutorService

import com.treode.async.{Callback, Scheduler}
import com.treode.async.io.File

trait Disks {

  def attach (items: Seq [(Path, File, DiskDriveConfig)], cb: Callback [Unit])

  def attach (items: Seq [(Path, DiskDriveConfig)], exec: ExecutorService, cb: Callback [Unit])

  def record [R] (desc: RecordDescriptor [R], entry: R, cb: Callback [Unit])

  def read [G, P] (desc: PageDescriptor [G, P], pos: Position, cb: Callback [P])

  def write [G, P] (desc: PageDescriptor [G, P], group: G, page: P, cb: Callback [Position])
}

object Disks {

  def recover () (implicit scheduler: Scheduler): Recovery =
    new RecoveryBuilder
}
