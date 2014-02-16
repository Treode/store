package com.treode.disk

import java.nio.file.Path
import java.util.concurrent.ExecutorService

import com.treode.async.{Async, Callback}
import com.treode.async.io.File

trait Recovery {

  def reload [B] (desc: RootDescriptor [B]) (f: B => Reload => Any)

  def replay [R] (desc: RecordDescriptor [R]) (f: R => Any)

  def launch (f: Launch => Any)

  def reattach (items: Seq [(Path, File)]): Async [Disks]

  def reattach (items: Seq [Path], executor: ExecutorService): Async [Disks]

  def attach (items: Seq [(Path, File, DiskGeometry)]): Async [Disks]

  def attach (items: Seq [(Path, DiskGeometry)], exec: ExecutorService): Async [Disks]
}
