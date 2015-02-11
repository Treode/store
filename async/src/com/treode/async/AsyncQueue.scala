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

package com.treode.async

/** Serialize asynchronous operations.
  *
  * Your class uses a fiber to serialize requests for work and manage data
  * structures that track the work to be done. The AsyncQueue runs one
  * asynchronous task at a time, and it asks your class to begin the next
  * asynchronous task when the first completes. Your class may receive multiple
  * requests from your user while an asynchronous task is underway. The
  * AsyncQueue allows your class to implement complex logic that prioritizes
  * and combines those requests.
  *
  * @param fiber The fiber your class uses to serialize requests.
  * @param reengage The function to begin the next asynchronous task.
  */
class AsyncQueue (fiber: Fiber) (reengage: () => Any) {

  private [this] var _engaged = true

  private def disengage(): Unit =
    fiber.execute {
      _engaged = false
      reengage()
    }

  /** Engage the queue, if not already engaged. If an asyncrhonous task is
    * already underway, this does nothing. Otherwise, this reengages the queue
    * to begin an asynchronous task (that is, it invokes `reengage`). Your
    * class should call this everytime it has added work to its queues.
    */
  def engage() {
    if (!_engaged)
      reengage()
  }

  /** Begin the next asynchronous task. Requires that this queue is not already
    * engaged in an asynchronous task. Launches the given task, and when it
    * completes, reengages the queue to run the next task (that is, it invokes
    * `reengage`). Your class should call this in response to reengage.
    */
  def begin [A] (task: => Async [A]) {
    require (_engaged == false)
    _engaged = true
    Async.guard (task) ensure (disengage()) run (Callback.ignore)
  }

  /** To be removed. */
  def engaged = _engaged

  /** To be removed. */
  def launch(): Unit =
    disengage()

  /** To be removed. */
  def execute (f: => Any): Unit =
    fiber.execute {
      f
      engage()
    }

  /** To be removed. */
  def async [A] (f: Callback [A] => Any): Async [A] =
    fiber.async  { cb =>
      f (cb)
      engage()
    }

  /** To be removed. */
  def run [A] (cb: Callback [A]) (task: => Async [A]) {
    require (_engaged == false)
    _engaged = true
    Async.guard (task) ensure (disengage()) run (cb)
  }}

object AsyncQueue {

  /** To be removed. */
  def apply (fiber: Fiber) (reengage: => Any) (implicit scheduler: Scheduler): AsyncQueue =
    new AsyncQueue (fiber) (() => reengage)
}
