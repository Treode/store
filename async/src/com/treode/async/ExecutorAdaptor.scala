package com.treode.async

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

private class ExecutorAdaptor (executor: ScheduledExecutorService) extends Scheduler {

  def execute (task: Runnable) =
    executor.execute (task)

  def execute (task: => Any) =
    executor.execute (toRunnable (task))

  def delay (millis: Long) (task: => Any) =
    executor.schedule (toRunnable (task), millis, TimeUnit.MILLISECONDS)

  def at (millis: Long) (task: => Any) {
    val t = System.currentTimeMillis
    if (millis < t)
      executor.execute (toRunnable (task))
    else
      executor.schedule (toRunnable (task), millis - t, TimeUnit.MILLISECONDS)
  }

  def spawn (task: => Any): Unit =
    executor.execute (toRunnable (task))
}
