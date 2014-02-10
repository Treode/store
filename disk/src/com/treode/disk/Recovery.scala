package com.treode.disk

import java.nio.file.Path
import java.util.concurrent.ExecutorService

import com.treode.async.Callback
import com.treode.async.io.File

trait Recovery {

  def reload [B] (desc: RootDescriptor [B]) (f: B => Reload => Any)

  def replay [R] (desc: RecordDescriptor [R]) (f: R => Any)

  def launch (f: Launch => Any)

  def reattach (items: Seq [(Path, File)], cb: Callback [Disks])

  def reattach (items: Seq [Path], executor: ExecutorService, cb: Callback [Disks])

  def attach (items: Seq [(Path, File, DiskGeometry)], cb: Callback [Disks])

  def attach (items: Seq [(Path, DiskGeometry)], exec: ExecutorService, cb: Callback [Disks])
}
