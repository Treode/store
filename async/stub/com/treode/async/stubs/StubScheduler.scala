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

import java.util.concurrent.ScheduledExecutorService
import scala.util.Random

import com.treode.async.Scheduler

trait StubScheduler extends Scheduler {

  def run (timers: => Boolean = false, count: Int = Int.MaxValue): Int
}

object StubScheduler {

  /** Run the tasks and timers one at a time in the thread that invokes the `run` method. Runs all
    * outstanding tasks, choosing the next task at random. If the timers parameter yields `true`,
    * then runs all outstanding timers too. Keeps going while there are outstanding tasks or timers
    * and `timers` yields `true`, or while there are outstanding `tasks` and `timers` yields false.
    *
    * This always uses a PRNG seeded with 0.
    */
  def random(): StubScheduler =
    new RandomScheduler (new Random (0))

  /** Run the tasks and timers one at a time in the thread that invokes the `run` method. Runs all
    * outstanding tasks, choosing the next task at random. If the timers parameter yields `true`,
    * then runs all outstanding timers too. Keeps going while there are outstanding tasks or timers
    * and `timers` yields `true`, or while there are outstanding `tasks` and `timers` yields false.
    *
    * You can run a test many times, each time providing a PRNG seeded from a different value, and
    * thereby choose different interleavings of tasks. This cannot detect all multithreading bugs,
    * but it can detect some, and it will surely boost your confidence that your code works.
    */
  def random (random: Random): StubScheduler =
    new RandomScheduler (random)

  /** Wrap the given executor. This scheduler runs the tasks in one or more threads separate from
    * from the thread that calls `run`. The `run` method periodically checks the result of the
    * `timers` parameter, and it loops while that method returns `true`. This scheduler runs per
    * the executor's timer regarless of the `timers` parameter.
    */
  def wrapped (executor: ScheduledExecutorService): StubScheduler =
    new StubExecutorAdaptor (executor)
}
