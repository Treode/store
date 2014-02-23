package com.treode.disk

import com.treode.async.{Async, AsyncConversions, Callback, Latch, Scheduler}
import com.treode.async.io.File

import Async.{async, guard}
import AsyncConversions._

private class ReloadAgent (
    files: Map [Int, File],
    roots: Seq [Disks.Reload => Async [Unit]],
    cb: Callback [Unit]
) (implicit
    scheduler: Scheduler
) extends Disks.Reload {

  def read [P] (desc: PageDescriptor [_, P], pos: Position): Async [P] =
    guard {
      DiskDrive.read (files (pos.disk), desc, pos)
    }

  roots.latch.unit (_ (this)) run (cb)
}
