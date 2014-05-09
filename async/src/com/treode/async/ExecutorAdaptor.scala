package com.treode.async

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

private class ExecutorAdaptor (executor: ScheduledExecutorService) extends Scheduler {

  def execute (task: Runnable) =
    executor.execute (task)

  def delay (millis: Long, task: Runnable) =
    executor.schedule (task, millis, TimeUnit.MILLISECONDS)

  def at (millis: Long, task: Runnable) {
    val t = System.currentTimeMillis
    if (millis < t)
      executor.execute (task)
    else
      executor.schedule (task, millis - t, TimeUnit.MILLISECONDS)
  }}
