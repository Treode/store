package com.treode.disk

import com.treode.async.Callback

trait Reload {

  def read [G, P] (desc: PageDescriptor [G, P], pos: Position, cb: Callback [P])

  def ready: Callback [Unit]
}
