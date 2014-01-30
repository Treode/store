package com.treode.disk

import com.treode.async.Callback

trait Reload {

  def read [P] (desc: PageDescriptor [_, P], pos: Position, cb: Callback [P])

  def ready: Callback [Unit]
}
