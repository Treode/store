package com.treode.disk

import com.treode.async.{Async, Callback}

trait Reload {

  def read [P] (desc: PageDescriptor [_, P], pos: Position): Async [P]
}
