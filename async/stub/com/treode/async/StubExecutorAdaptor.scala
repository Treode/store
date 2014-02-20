package com.treode.async

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

private class StubExecutorAdaptor (executor: ScheduledExecutorService)
extends ExecutorAdaptor (executor) with StubScheduler {

  def runTasks (timers: Boolean, count: Int): Int = -1

  def shutdown (timeout: Long) {
    executor.shutdown()
    executor.awaitTermination (timeout, TimeUnit.MILLISECONDS)
  }}
