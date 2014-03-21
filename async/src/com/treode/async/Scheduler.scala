package com.treode.async

import java.util.concurrent.{Executor, TimeUnit, ScheduledExecutorService}
import scala.runtime.NonLocalReturnControl
import scala.util.{Failure, Success, Try}

import Scheduler.toRunnable

trait Scheduler extends Executor {

  def execute (task: Runnable)

  def delay (millis: Long, task: Runnable)

  def at (millis: Long, task: Runnable)

  def execute (task: => Any): Unit =
    execute (toRunnable (task))

  def execute [A] (f: A => Any, v: A): Unit =
    execute (toRunnable (f, v))

  def execute [A] (cb: Callback [A], v: Try [A]): Unit =
    execute (toRunnable (cb, v))

  def delay (millis: Long) (task: => Any): Unit =
    delay (millis, toRunnable (task))

  def at (millis: Long) (task: => Any): Unit =
    at (millis, toRunnable (task))

  def pass [A] (cb: Callback [A], v: A): Unit =
    execute (cb, Success (v))

  def fail [A] (cb: Callback [A], t: Throwable): Unit =
    execute (cb, Failure (t))

  val whilst = new Whilst (this)
}

object Scheduler {

  def apply (executor: ScheduledExecutorService): Scheduler =
    new ExecutorAdaptor (executor)

  def toRunnable (task: => Any): Runnable =
    new Runnable {
      def run() =
        try {
          task
        } catch {
          case t: NonLocalReturnControl [_] => ()
        }}

  def toRunnable [A] (f: A => Any, v: A): Runnable =
    new Runnable {
      def run() = f (v)
    }

  def toRunnable [A] (cb: Callback [A], v: Try [A]): Runnable =
    new Runnable {
      def run() = cb (v)
    }}
