/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

  def run (cond: => Boolean, count: Int): Int = {
    var n = 0
    while (n < count && (!tasks.isEmpty || cond && !timers.isEmpty)) {
      if (tasks.isEmpty)
        nextTimer()
      else
        nextTask()
      n += 1
    }
    n
  }}
