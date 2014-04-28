package com.treode.disk

import java.util.ArrayList
import scala.collection.JavaConversions

import com.treode.async.{Async, Callback, Scheduler}

import JavaConversions._

private class LaunchAgent (val kit: DisksKit) extends Disks.Launch {

  private val roots = new CheckpointRegistry
  private val pages = new PageRegistry (kit)
  private var open = true

  implicit val disks: Disks = new DisksAgent (kit)

  val controller: Disks.Controller = new ControllerAgent (kit, disks)

  def requireOpen(): Unit =
    require (open, "Disks have already launched.")

  def checkpoint (f: => Async [Unit]): Unit =
    synchronized {
      requireOpen()
      roots.checkpoint (f)
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
      kit.launch (roots, pages)
    }}
