package com.treode.disk.stubs

import com.treode.async.Async
import com.treode.disk._

import Disks.{Controller, Launch}

private class StubLaunchAgent (val disks: StubDisks) extends Launch {

  private val roots = new CheckpointRegistry
  private var open = true

  def requireOpen(): Unit =
    require (open, "Disks have already launched.")

  def checkpoint (f: => Async [Unit]): Unit =
    synchronized {
      requireOpen()
      roots.checkpoint (f)
    }

  def handle [G] (desc: PageDescriptor [G, _], handler: PageHandler [G]): Unit =
    ()

  def launch(): Unit =
    synchronized {
      requireOpen()
      open = false
      disks.launch (roots)
    }

  def controller: Controller =
    throw new UnsupportedOperationException ("The StubDisks do not provide a controller.")
}
