package com.treode.disk

import java.nio.file.Path
import java.util.concurrent.ExecutorService

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.io.File

trait Disks {

  def record [R] (desc: RecordDescriptor [R], entry: R): Async [Unit]

  def read [P] (desc: PageDescriptor [_, P], pos: Position): Async [P]

  def write [G, P] (desc: PageDescriptor [G, P], obj: ObjectId, group: G, page: P): Async [Position]

  def compact (desc: PageDescriptor [_, _], obj: ObjectId): Async [Unit]

  def join [A] (task: Async [A]): Async [A]
}

object Disks {

  trait Controller {

    implicit def disks: Disks

    def _attach (items: (Path, File, DiskGeometry)*): Async [Unit]

    def attach (exec: ExecutorService, items: (Path, DiskGeometry)*): Async [Unit]

    def drain (items: Path*): Async [Unit]
  }

  trait Launch {

    implicit def disks: Disks

    implicit def controller: Controller

    def checkpoint (f: => Async [Unit])

    def handle [G] (desc: PageDescriptor [G, _], handler: PageHandler [G])

    def launch()
  }

  trait Recovery {

    def replay [R] (desc: RecordDescriptor [R]) (f: R => Any)

    def _reattach (items: (Path, File)*): Async [Launch]

    def reattach (exec: ExecutorService, items: Path*): Async [Launch]

    def _attach (items: (Path, File, DiskGeometry)*): Async [Launch]

    def attach (exec: ExecutorService, items: (Path, DiskGeometry)*): Async [Launch]
  }

  def recover () (implicit scheduler: Scheduler, config: DisksConfig): Recovery =
    new RecoveryAgent
}
