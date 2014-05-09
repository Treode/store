package com.treode.disk

import java.nio.file.Path
import com.treode.async.{Async, Callback, Scheduler}

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

    def attach (items: (Path, DiskGeometry)*): Async [Unit]

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

    def reattach (items: Path*): Async [Launch]

    def attach (items: (Path, DiskGeometry)*): Async [Launch]
  }

  def recover () (implicit scheduler: Scheduler, config: DisksConfig): Recovery =
    new RecoveryAgent
}
