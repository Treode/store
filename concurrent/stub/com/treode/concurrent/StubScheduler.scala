package com.treode.concurrent

import java.util.ArrayDeque
import scala.collection.mutable.PriorityQueue

class StubScheduler extends Scheduler {
  import StubScheduler.Timer

  private val tasks = new ArrayDeque [Runnable]
  private val timers = new PriorityQueue [Timer]

  var time = 0

  def execute (task: Runnable): Unit =
    tasks.add (task)

  def execute (task: => Any): Unit =
    tasks.add (toRunnable (task))

  def delay (millis: Long) (task: => Any): Unit =
    timers.enqueue (Timer (time + millis, toRunnable (task)))

  def at (millis: Long) (task: => Any): Unit =
    timers.enqueue (Timer (millis, toRunnable (task)))

  def spawn (task: => Any): Unit =
    tasks.add (toRunnable (task))

  def isTasksEmpty: Boolean =
    tasks.isEmpty

  def nextTask(): Unit =
    tasks.remove().run()

  def runTasks(): Unit =
    while (!isTasksEmpty)
      nextTask()

  def nextTimer(): Unit =
    timers.dequeue().task.run()
}

object StubScheduler {

  private [concurrent] case class Timer (time: Long, task: Runnable) extends Ordered [Timer] {

    def compare (that: Timer) = this.time compare that.time
  }

  private [concurrent] object Timer extends Ordering [Timer] {

    def compare (x: Timer, y: Timer) = x compare y
  }}
