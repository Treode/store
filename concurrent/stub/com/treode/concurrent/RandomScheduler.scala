package com.treode.concurrent

import scala.collection.mutable.{ArrayBuffer, PriorityQueue}
import scala.util.Random

private class RandomScheduler (random: Random) extends StubScheduler {

  private val tasks = new ArrayBuffer [Runnable]
  private val timers = new PriorityQueue [StubTimer]

  private var time = 0L

  def execute (task: Runnable): Unit =
    tasks.append (task)

  def execute (task: => Any): Unit =
    tasks.append (toRunnable (task))

  def delay (millis: Long) (task: => Any): Unit =
    timers.enqueue (StubTimer (time + millis, toRunnable (task)))

  def at (millis: Long) (task: => Any): Unit =
    timers.enqueue (StubTimer (millis, toRunnable (task)))

  def spawn (task: => Any): Unit =
    tasks.append (toRunnable (task))

  def nextTask(): Unit = {
    val i = random.nextInt (tasks.size)
    val t = tasks (i)
    tasks (i) = tasks (tasks.size-1)
    tasks.reduceToSize (tasks.size-1)
    t.run()
  }

  def nextTimer() {
    val t = timers.dequeue()
    time = t.time
    t.task.run()
  }

  def runTasks (withTimers: Boolean) {
    while (!tasks.isEmpty || withTimers && !timers.isEmpty) {
      if (tasks.isEmpty)
        nextTimer()
      else
        nextTask()
    }}

  def shutdown (timeout: Long) = ()
}
