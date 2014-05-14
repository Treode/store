package com.treode.disk.stubs

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.disk._

import Disks.{Controller, Launch}

private class StubLaunchAgent (
    releaser: StubReleaser,
    val disks: StubDisks
) (implicit
    random: Random,
    scheduler: Scheduler,
    disk: StubDiskDrive,
    config: StubConfig
) extends Launch {

  private val roots = new CheckpointRegistry
  private val pages = new StubPageRegistry (releaser)
  private var open = true

  def requireOpen(): Unit =
    require (open, "Disks have already launched.")

  def checkpoint (f: => Async [Unit]): Unit =
    synchronized {
      requireOpen()
      roots.checkpoint (f)
    }

  def handle [G] (desc: PageDescriptor [G, _], handler: PageHandler [G]): Unit =
    pages.handle (desc, handler)

  def launch(): Unit =
    synchronized {
      requireOpen()
      open = false
      disks.launch (roots, pages)
    }

  def controller: Controller =
    throw new UnsupportedOperationException ("The StubDisks do not provide a controller.")
}
