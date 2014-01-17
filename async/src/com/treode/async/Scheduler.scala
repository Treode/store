package com.treode.async

import java.util.concurrent.{Executor, TimeUnit, ScheduledExecutorService}

trait Scheduler extends Executor {

  def execute (task: Runnable)

  def delay (millis: Long, task: Runnable)

  def at (millis: Long, task: Runnable)

  def spawn (task: Runnable)

  def execute (task: => Any): Unit =
    execute (toRunnable (task))

  def delay (millis: Long) (task: => Any): Unit =
    delay (millis, toRunnable (task))

  def at (millis: Long) (task: => Any): Unit =
    at (millis, toRunnable (task))

  def spawn (task: => Any): Unit =
    spawn (toRunnable (task))

  def execute [A] (cb: Callback [A], v: A): Unit =
    execute (toRunnable (cb, v))

  def spawn [A] (cb: Callback [A], v: A): Unit =
    spawn (toRunnable (cb, v))

  def fail [A] (cb: Callback [A], t: Throwable): Unit =
    spawn (toRunnable (cb, t))
}

object Scheduler {

  def apply (executor: ScheduledExecutorService): Scheduler =
    new ExecutorAdaptor (executor)
}
