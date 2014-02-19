package com.treode.async

import java.util.ArrayDeque

class Fiber (scheduler: Scheduler) extends Scheduler {

  private [this] val tasks = new ArrayDeque [Runnable]
  private [this] var engaged = false

  private [this] def disengage(): Unit = synchronized {
    if (!tasks.isEmpty)
      scheduler.execute (tasks.remove)
    else
      engaged = false
  }

  private [this] def add (task: Runnable): Unit = synchronized {
    if (engaged) {
      tasks.add (task)
    } else {
      engaged = true
      scheduler.execute (task)
    }}

  def execute (task: Runnable): Unit =
    add (new Runnable {
      def run() {
        try {
          task.run()
        } finally {
          disengage()
        }}})

  def delay (millis: Long, task: Runnable): Unit =
    scheduler.delay (millis) (execute (task))

  def at (millis: Long, task: Runnable): Unit =
    scheduler.at (millis) (execute (task))

  def async [A] (f: Callback [A] => Any): Async [A] =
    Async.async { cb =>
      execute {
        try {
          f (cb)
        } catch {
          case t: Throwable => scheduler.fail (cb, t)
        }}}

  def guard [A] (f: => Async [A]): Async [A] =
    Async.async [Async [A]] { cb =>
      execute {
        try {
          scheduler.pass (cb, f)
        } catch {
          case t: Throwable => scheduler.fail (cb, t)
        }}} .flatten

  def run [A] (cb: Callback [A]) (f: => Async [A]): Unit =
    execute {
      try {
        f run (spawn (cb))
      } catch {
        case t: Throwable =>
          scheduler.fail (cb, t)
      }}

  def defer [A] (cb: Callback [A]) (f: => Any): Unit =
    execute {
      try {
        f
      } catch {
        case t: Throwable => scheduler.fail (cb, t)
      }}

  def supply [A] (f: => A): Async [A] =
    Async.async { cb =>
      execute {
        try {
          scheduler.pass (cb, f)
        } catch {
          case t: Throwable => scheduler.fail (cb, t)
        }}}

  def spawn (task: Runnable): Unit =
    scheduler.execute (task)

  def spawn (task: => Any): Unit =
    scheduler.execute (task)

  def spawn [A] (cb: Callback [A]): Callback [A] =
    scheduler.take (cb)
}
