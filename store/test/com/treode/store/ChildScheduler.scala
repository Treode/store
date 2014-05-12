package com.treode.store

import com.treode.async.Scheduler

class ChildScheduler (parent: Scheduler) extends Scheduler {

  private var running = true

  private def check (task: Runnable): Runnable =
    new Runnable {
      def run(): Unit = if (running) task.run()
    }

  def execute (task: Runnable): Unit =
    parent.execute (check (task))

  def delay (millis: Long, task: Runnable): Unit =
    parent.delay (millis, check (task))

  def at (millis: Long, task: Runnable): Unit =
    parent.at (millis, check (task))

  def shutdown(): Unit =
    running = false
}
