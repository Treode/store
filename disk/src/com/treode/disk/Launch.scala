package com.treode.disk

import com.treode.async.Callback

trait Launch {

  implicit def disks: Disks

  def read [P] (desc: PageDescriptor [_, P], pos: Position, cb: Callback [P])

  def checkpoint [B] (desc: RootDescriptor [B]) (f: Callback [B] => Any)

  def handle [G] (desc: PageDescriptor [G, _], handler: PageHandler [G])

  def ready: Callback [Unit]
}
