package com.treode.disk

import java.nio.file.Path
import java.util.concurrent.ExecutorService
import scala.reflect.ClassTag

import com.google.common.annotations.VisibleForTesting
import com.treode.async.{Callback, Scheduler}
import com.treode.async.io.File
import com.treode.pickle.Pickler

trait Disks {

  def reattach (items: Seq [(Path, File)], cb: Callback [Unit])
  def reattach (items: Seq [Path], executor: ExecutorService, cb: Callback [Unit])
  def attach (items: Seq [(Path, File, DiskDriveConfig)], cb: Callback [Unit])
  def attach (items: Seq [(Path, DiskDriveConfig)], exec: ExecutorService, cb: Callback [Unit])

  def register [R] (p: Pickler [R], id: TypeId) (f: R => Any)
  def record [R] (p: Pickler [R], id: TypeId, entry: R, cb: Callback [Unit])

  def read [G, P] (desc: PageDescriptor [G, P], pos: Position, cb: Callback [P])
  def write [G, P] (desc: PageDescriptor [G, P], group: G, page: P, cb: Callback [Position])
}

object Disks {

  def apply (scheduler: Scheduler): Disks =
    new DisksKit (scheduler)
}
