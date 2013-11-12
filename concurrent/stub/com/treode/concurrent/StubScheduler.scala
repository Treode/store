package com.treode.concurrent

import scala.collection.mutable.{ArrayBuffer, PriorityQueue}
import scala.util.Random

class StubScheduler private (random: Random) extends Scheduler {

  private val tasks = new ArrayBuffer [Runnable]
  private val timers = new PriorityQueue [Timer]

  private var time = 0

  def execute (task: Runnable): Unit =
    tasks.append (task)

  def execute (task: => Any): Unit =
    tasks.append (toRunnable (task))

  def delay (millis: Long) (task: => Any): Unit =
    timers.enqueue (Timer (time + millis, toRunnable (task)))

  def at (millis: Long) (task: => Any): Unit =
    timers.enqueue (Timer (millis, toRunnable (task)))

  def spawn (task: => Any): Unit =
    tasks.append (toRunnable (task))

  def isTasksEmpty: Boolean =
    tasks.isEmpty

  def nextTask(): Unit = {
    val i = random.nextInt (tasks.size)
    val t = tasks (i)
    tasks (i) = tasks (tasks.size-1)
    tasks.reduceToSize (tasks.size-1)
    t.run()
  }

  def runTasks(): Unit =
    while (!isTasksEmpty)
      nextTask()

  def nextTimer(): Unit =
    timers.dequeue().task.run()
}

object StubScheduler {

  def apply(): StubScheduler =
    new StubScheduler (new Random (0))

  def apply (random: Random): StubScheduler =
    new StubScheduler (random)
}
