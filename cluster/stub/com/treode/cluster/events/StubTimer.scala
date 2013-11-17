package com.treode.cluster.events

import com.codahale.metrics.Timer

object StubTimer extends Timer {

  def time [A] (f: => A): A = f
}
