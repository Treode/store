package com.treode.disk

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.async.{Callback, Scheduler, delay}

private class LaunchAgent (
    drives: DiskDrives,
    launches: ArrayList [Launch => Any],
    cb: Callback [Disks]) (
        implicit scheduler: Scheduler) extends Launch {

  val roots = new CheckpointRegistry () (disks)
  val pages = new PageRegistry

  def disks: Disks = drives

  def read [P] (desc: PageDescriptor [_, P], pos: Position, cb: Callback [P]): Unit =
    drives.fetch (desc, pos, cb)

  def checkpoint [B] (desc: RootDescriptor [B]) (f: Callback [B] => Any): Unit =
    roots.checkpoint (desc) (f)

  def handle [G] (desc: PageDescriptor [G, _], handler: PageHandler [G]): Unit =
    pages.handle (desc, handler)

  val launched = delay (cb) { _: Unit =>
    drives.launch (roots, pages)
    scheduler.execute (cb, drives)
  }
  val ready = Callback.latch (launches.size, launched)
  launches foreach (f => scheduler.execute (f (this)))
}
