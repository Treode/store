package com.treode.disk.stubs

import com.treode.async.Async
import com.treode.disk.{Disks, PageDescriptor, PageHandler}

import Disks.{Controller, Launch}

private class StubLaunchAgent (delegate: Launch) extends Launch {

  def disks: Disks =
    delegate.disks

  def checkpoint (f: => Async [Unit]): Unit =
    delegate.checkpoint (f)

  def handle [G] (desc: PageDescriptor [G, _], handler: PageHandler [G]): Unit =
    delegate.handle (desc, handler)

  def launch(): Unit =
    delegate.launch()

  def controller: Controller =
    throw new UnsupportedOperationException ("The StubDisks do not provide a controller.")
}
