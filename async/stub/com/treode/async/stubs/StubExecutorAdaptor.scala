package com.treode.async.stubs

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import com.treode.async.ExecutorAdaptor

private class StubExecutorAdaptor (executor: ScheduledExecutorService)
extends ExecutorAdaptor (executor) with StubScheduler {

  def run (timers: => Boolean, count: Int, oblivious: Boolean): Int = {
    var n = 0
    while (n < count && timers) {
      Thread.sleep (10)
      n += 1
    }
    n
  }}
