package com.treode.async

import java.util

class Fiber (scheduler: Scheduler) extends Scheduler {

  private [this] val tasks = new util.ArrayDeque [Runnable]
  private [this] var engaged: Boolean = false

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

  def begin (task: Callback [Unit] => Any): Unit =
    add (new Runnable {
      def run() {
        val cb = new Callback [Unit] {
          def pass (v: Unit): Unit = disengage()
          def fail (t: Throwable) {
            disengage()
            throw t
          }}
        try {
          task (cb)
        } catch {
          case t: Throwable => cb.fail (t)
        }}})

  def delay (millis: Long, task: Runnable): Unit =
    scheduler.delay (millis) (execute (task))

  def at (millis: Long, task: Runnable): Unit =
    scheduler.at (millis) (execute (task))

  def spawn (task: Runnable): Unit =
    scheduler.execute (task)
}
