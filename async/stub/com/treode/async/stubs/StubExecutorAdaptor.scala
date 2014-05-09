package com.treode.async.stubs

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import com.treode.async.ExecutorAdaptor

private class StubExecutorAdaptor (executor: ScheduledExecutorService)
extends ExecutorAdaptor (executor) with StubScheduler {

  def runTasks (timers: Boolean, count: Int): Int = -1
}
