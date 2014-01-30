package com.treode.disk

import com.treode.async.{Callback, Scheduler}

private class ReloadAgent (
    val disks: DiskDrives,
    roots: Seq [Reload => Any],
    cb: Callback [Unit]) (
        implicit scheduler: Scheduler) extends Reload {

  def read [G, P] (desc: PageDescriptor [G, P], pos: Position, cb: Callback [P]): Unit =
    disks.fetch (desc, pos, cb)

  val ready = Callback.latch (roots.size, cb)
  roots foreach (f => scheduler.execute (f (this)))
}
