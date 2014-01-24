package com.treode.disk

import java.nio.file.Path
import java.util.concurrent.ExecutorService

import com.treode.async.{Callback, Scheduler}
import com.treode.async.io.File

trait Disks {

  def reattach (items: Seq [(Path, File)], cb: Callback [Unit])

  def reattach (items: Seq [Path], executor: ExecutorService, cb: Callback [Unit])

  def attach (items: Seq [(Path, File, DiskDriveConfig)], cb: Callback [Unit])

  def attach (items: Seq [(Path, DiskDriveConfig)], exec: ExecutorService, cb: Callback [Unit])

  def open [B] (desc: RootDescriptor [B]) (f: Recovery => Any)

  def checkpoint [B] (desc: RootDescriptor [B]) (f: Callback [B] => Any)

  def record [R] (desc: RecordDescriptor [R], entry: R, cb: Callback [Unit])

  def read [G, P] (desc: PageDescriptor [G, P], pos: Position, cb: Callback [P])

  def write [G, P] (desc: PageDescriptor [G, P], group: G, page: P, cb: Callback [Position])
}

object Disks {

  def apply () (implicit scheduler: Scheduler): Disks =
    new DisksKit
}
