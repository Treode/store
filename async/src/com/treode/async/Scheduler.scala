package com.treode.async

import java.util.concurrent.{Executor, TimeUnit, ScheduledExecutorService}

import Scheduler.toRunnable

trait Scheduler extends Executor {

  def execute (task: Runnable)

  def delay (millis: Long, task: Runnable)

  def at (millis: Long, task: Runnable)

  def execute (task: => Any): Unit =
    execute (toRunnable (task))

  def delay (millis: Long) (task: => Any): Unit =
    delay (millis, toRunnable (task))

  def at (millis: Long) (task: => Any): Unit =
    at (millis, toRunnable (task))

  def pass [A] (cb: Callback [A], v: A): Unit =
    execute (toRunnable (cb, v))

  def fail (cb: Callback [_], t: Throwable): Unit =
    execute (toRunnable (cb, t))

  def take [A] (cb: Callback [A]): Callback [A] =
    new Callback [A] {
      def pass (v: A): Unit = execute (toRunnable (cb, v))
      def fail (t: Throwable): Unit = execute (toRunnable (cb, t))
    }}

object Scheduler {

  def apply (executor: ScheduledExecutorService): Scheduler =
    new ExecutorAdaptor (executor)

  def toRunnable (task: => Any): Runnable =
    new Runnable {
      def run() = task
    }

  def toRunnable [A] (f: A => Any, v: A): Runnable =
    new Runnable {
      def run() = f (v)
    }

  def toRunnable [A] (cb: Callback [A], v: A): Runnable =
    new Runnable {
      def run() = cb (v)
    }

  def toRunnable [A] (cb: Callback [A], t: Throwable): Runnable =
    new Runnable {
      def run() = cb.fail (t)
    }
}
