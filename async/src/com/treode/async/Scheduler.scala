package com.treode.async

import java.util.concurrent.{TimeUnit, ScheduledExecutorService}

trait Scheduler {

  def execute (task: Runnable)

  def execute (task: => Any)

  def delay (millis: Long) (task: => Any)

  def at (millis: Long) (task: => Any)

  def spawn (task: => Any)
}

object Scheduler {

  def apply (executor: ScheduledExecutorService): Scheduler =
    new ExecutorAdaptor (executor)
}
