package com.treode.disk

import com.treode.async.{Async, Callback, Latch, Scheduler}
import com.treode.async.io.File

import Async.guard

private class ReloadAgent (
    files: Map [Int, File],
    roots: Seq [Reload => Any],
    cb: Callback [Unit]
) (implicit
    scheduler: Scheduler
) extends Reload {

  def read [P] (desc: PageDescriptor [_, P], pos: Position): Async [P] =
    guard {
      DiskDrive.read (files (pos.disk), desc, pos)
    }

  val ready = Latch.unit (roots.size, cb)
  roots foreach (f => scheduler.execute (f (this)))
}
