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
