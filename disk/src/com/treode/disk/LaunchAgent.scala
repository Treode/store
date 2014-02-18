package com.treode.disk

import java.util.ArrayList
import scala.collection.JavaConversions

import com.treode.async.{Async, AsyncConversions, Callback, Scheduler}

import AsyncConversions._
import Callback.continue
import JavaConversions._

private class LaunchAgent (
    drives: DiskDrives,
    launches: ArrayList [Launch => Async [Unit]],
    cb: Callback [Disks]) (
        implicit scheduler: Scheduler) extends Launch {

  val roots = new CheckpointRegistry () (drives)
  val pages = new PageRegistry (drives)

  def disks: Disks = drives

  def read [P] (desc: PageDescriptor [_, P], pos: Position): Async [P] =
    drives.fetch (desc, pos)

  def checkpoint [B] (desc: RootDescriptor [B]) (f: => Async [B]): Unit =
    roots.checkpoint (desc) (f)

  def handle [G] (desc: PageDescriptor [G, _], handler: PageHandler [G]): Unit =
    pages.handle (desc, handler)

  val task = for {
    _ <- launches.latch.unit (_ (this))
  } yield {
    drives.launch (roots, pages)
    scheduler.pass (cb, drives)
  }
  task defer cb
}
