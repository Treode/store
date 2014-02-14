package com.treode.disk

import com.treode.async.{Callback, Latch, Scheduler, defer}
import com.treode.async.io.File

private class ReloadAgent (
    files: Map [Int, File],
    roots: Seq [Reload => Any],
    cb: Callback [Unit]) (
        implicit scheduler: Scheduler) extends Reload {

  def read [P] (desc: PageDescriptor [_, P], pos: Position, cb: Callback [P]): Unit =
    defer (cb) {
      DiskDrive.read (files (pos.disk), desc, pos, cb)
    }

  val ready = Latch.unit (roots.size, cb)
  roots foreach (f => scheduler.execute (f (this)))
}
