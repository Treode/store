package com.treode.disk.stubs

import com.treode.async.Async
import com.treode.disk.{Disks, PageDescriptor, PageHandler}

import Disks.{Controller, Launch}

private class StubLaunchAgent (val disks: StubDisks) extends Launch {

  def checkpoint (f: => Async [Unit]): Unit =
    ()

  def handle [G] (desc: PageDescriptor [G, _], handler: PageHandler [G]): Unit =
    ()

  def launch(): Unit =
    ()

  def controller: Controller =
    throw new UnsupportedOperationException ("The StubDisks do not provide a controller.")
}
