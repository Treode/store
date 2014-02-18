package com.treode.disk

import com.treode.async.{Async, Callback}

trait Launch {

  implicit def disks: Disks

  def read [P] (desc: PageDescriptor [_, P], pos: Position): Async [P]

  def checkpoint [B] (desc: RootDescriptor [B]) (f: => Async [B])

  def handle [G] (desc: PageDescriptor [G, _], handler: PageHandler [G])

  def ready: Callback [Unit]
}
