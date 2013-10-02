package com.treode.cluster.concurrent

import java.util

class Fiber (scheduler: Scheduler) extends Scheduler {

  private[this] val tasks = new util.ArrayDeque [Runnable]
  private[this] var engaged: Boolean = false

  private[this] def process (task: Runnable) {
    scheduler.execute (new Runnable {
      def run() {
        try {
          task.run()
        }
        finally {
          disengage()
        }}})
  }

  private[this] def engage() {
    engaged = true
    process (tasks.remove)
  }

  private[this] def disengage(): Unit = synchronized {
    if (!tasks.isEmpty)
      process (tasks.remove)
    else
      engaged = false
  }

  def execute (task: Runnable): Unit = synchronized {
    tasks.add (task)
    if (!engaged)
      engage()
  }

  def execute (task: => Any): Unit =
    execute (new Runnable {
      def run() = task
    })

  def delay (millis: Long) (task: => Any): Unit =
    scheduler.delay (millis) (execute (task))

  def at (millis: Long) (task: => Any): Unit =
    scheduler.at (millis) (execute (task))

  def spawn (task: => Any): Unit =
    scheduler.execute (task)
}
