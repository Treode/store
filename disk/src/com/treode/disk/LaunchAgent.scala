package com.treode.disk

import java.util.ArrayList
import scala.collection.JavaConversions

import com.treode.async.{Async, AsyncConversions, Callback, Scheduler}

import AsyncConversions._
import JavaConversions._

private class LaunchAgent (drives: DiskDrives) extends Disks.Launch {

  val roots = new CheckpointRegistry () (drives)
  val pages = new PageRegistry (drives)
  private var open = true

  def requireOpen(): Unit =
    require (open, "Disks have already launched.")

  def disks: Disks = drives

  def checkpoint [B] (desc: RootDescriptor [B]) (f: => Async [B]): Unit =
    synchronized {
      requireOpen()
      roots.checkpoint (desc) (f)
    }

  def handle [G] (desc: PageDescriptor [G, _], handler: PageHandler [G]): Unit =
    synchronized {
      requireOpen()
      pages.handle (desc, handler)
    }

  def launch(): Unit =
    synchronized {
      requireOpen()
      open = false
      drives.launch (roots, pages)
    }}
