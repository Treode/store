package com.treode.async

import java.util.ArrayDeque
import scala.runtime.NonLocalReturnControl

import com.treode.async.implicits._

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
          case t: NonLocalReturnControl [_] => scheduler.fail (cb, new ReturnNotAllowedFromAsync)
          case t: Throwable => scheduler.fail (cb, t)
        }}}

  def guard [A] (f: => Async [A]): Async [A] =
    Async.async [Async [A]] { cb =>
      execute {
        try {
          scheduler.pass (cb, f)
        } catch {
          case t: NonLocalReturnControl [_] => scheduler.pass (cb, t.value.asInstanceOf [Async [A]])
          case t: Throwable => scheduler.fail (cb, t)
        }}} .flatten

  def supply [A] (f: => A): Async [A] =
    Async.async { cb =>
      execute {
        try {
          scheduler.pass (cb, f)
        } catch {
          case t: NonLocalReturnControl [_] => scheduler.fail (cb, new ReturnNotAllowedFromAsync)
          case t: Throwable => scheduler.fail (cb, t)
        }}}}
