package com.treode.disk

import com.treode.async.Callback

trait Launch {

  implicit def disks: Disks

  def read [G, P] (desc: PageDescriptor [G, P], pos: Position, cb: Callback [P])

  def checkpoint [B] (desc: RootDescriptor [B]) (f: Callback [B] => Any)

  def handle [G, P] (desc: PageDescriptor [G, P], handler: PageHandler [G])

  def ready: Callback [Unit]
}
