package com.treode.disk

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.async.{Async, Callback, Latch, Scheduler, continue}

private class LaunchAgent (
    drives: DiskDrives,
    launches: ArrayList [Launch => Any],
    cb: Callback [Disks]) (
        implicit scheduler: Scheduler) extends Launch {

  val roots = new CheckpointRegistry () (drives)
  val pages = new PageRegistry (drives)

  def disks: Disks = drives

  def read [P] (desc: PageDescriptor [_, P], pos: Position, cb: Callback [P]): Unit =
    drives.fetch (desc, pos, cb)

  def checkpoint [B] (desc: RootDescriptor [B]) (f: => Async [B]): Unit =
    roots.checkpoint (desc) (f)

  def handle [G] (desc: PageDescriptor [G, _], handler: PageHandler [G]): Unit =
    pages.handle (desc, handler)

  val launched = continue (cb) { _: Unit =>
    drives.launch (roots, pages)
    scheduler.pass (cb, drives)
  }
  val ready = Latch.unit (launches.size, launched)
  launches foreach (f => scheduler.execute (f (this)))
}
