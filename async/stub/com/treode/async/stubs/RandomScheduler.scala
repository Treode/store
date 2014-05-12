package com.treode.async.stubs

import scala.collection.mutable.{ArrayBuffer, PriorityQueue}
import scala.util.Random

private class RandomScheduler (random: Random) extends StubScheduler {

  private val tasks = new ArrayBuffer [Runnable]
  private val timers = new PriorityQueue [StubTimer]

  private var time = 0L

  def execute (task: Runnable): Unit =
    tasks.append (task)

  def delay (millis: Long, task: Runnable): Unit =
    timers.enqueue (StubTimer (time + millis, task))

  def at (millis: Long, task: Runnable): Unit =
    timers.enqueue (StubTimer (millis, task))

  def spawn (task: Runnable): Unit =
    tasks.append (task)

  def nextTask (oblivious: Boolean): Unit = {
    val i = random.nextInt (tasks.size)
    val t = tasks (i)
    tasks (i) = tasks (tasks.size-1)
    tasks.reduceToSize (tasks.size-1)
    try {
      t.run()
    } catch {
      case _: Throwable if oblivious => ()
    }}

  def nextTimer (oblivious: Boolean) {
    val t = timers.dequeue()
    time = t.time
    try {
      t.task.run()
    } catch {
      case _: Throwable if oblivious => ()
    }}

  def run (cond: => Boolean, count: Int, oblivious: Boolean): Int = {
    var n = 0
    while (n < count && (!tasks.isEmpty || cond && !timers.isEmpty)) {
      if (tasks.isEmpty)
        nextTimer (oblivious)
      else
        nextTask (oblivious)
      n += 1
    }
    n
  }}
