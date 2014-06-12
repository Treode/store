package com.treode.disk

import java.nio.file.Path
import com.treode.async.{Async, Callback, Scheduler}

trait Disk {

  def record [R] (desc: RecordDescriptor [R], entry: R): Async [Unit]

  def read [P] (desc: PageDescriptor [_, P], pos: Position): Async [P]

  def write [G, P] (desc: PageDescriptor [G, P], obj: ObjectId, group: G, page: P): Async [Position]

  def compact (desc: PageDescriptor [_, _], obj: ObjectId): Async [Unit]

  def join [A] (task: Async [A]): Async [A]
}

object Disk {

  trait Controller {

    implicit def disk: Disk

    def attach (items: (Path, DiskGeometry)*): Async [Unit]

    def drain (items: Path*): Async [Unit]

    def shutdown(): Async [Unit]
  }

  trait Launch {

    implicit def disk: Disk

    implicit def controller: Controller

    def sysid: Array [Byte]

    def checkpoint (f: => Async [Unit])

    def handle [G] (desc: PageDescriptor [G, _], handler: PageHandler [G])

    def launch()
  }

  trait Recovery {

    def replay [R] (desc: RecordDescriptor [R]) (f: R => Any)

    def reattach (items: Path*): Async [Launch]
  }

  def init (
      sysid: Array [Byte],
      superBlockBits: Int,
      segmentBits: Int,
      blockBits: Int,
      diskBytes: Long,
      paths: Path*
  ): Unit =
    DiskDrive.init (sysid, superBlockBits, segmentBits, blockBits, diskBytes, paths)


  def recover () (implicit scheduler: Scheduler, config: DiskConfig): Recovery =
    new RecoveryAgent
}
