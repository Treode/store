package com.treode.disk

import java.nio.file.Path
import java.util.concurrent.ExecutorService

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.io.File

trait Disks {

  def attach (items: Seq [(Path, File, DiskGeometry)]): Async [Unit]

  def attach (items: Seq [(Path, DiskGeometry)], exec: ExecutorService): Async [Unit]

  def record [R] (desc: RecordDescriptor [R], entry: R): Async [Unit]

  def read [P] (desc: PageDescriptor [_, P], pos: Position): Async [P]

  def write [G, P] (desc: PageDescriptor [G, P], group: G, page: P): Async [Position]

  def join [A] (task: Async [A]): Async [A]
}

object Disks {

  def recover () (implicit scheduler: Scheduler, config: DisksConfig): Recovery =
    new RecoveryBuilder
}
