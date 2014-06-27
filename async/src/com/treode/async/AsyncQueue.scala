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

/** Experimental. */
class AsyncQueue (fiber: Fiber) (deque: => Option [Runnable]) {

  private [this] var _engaged = true

  private def _reengage() {
    deque match {
      case Some (task) =>
        _engaged = true
        task.run()
      case None =>
        _engaged = false
    }}

  private def reengage(): Unit =
    fiber.execute {
      _reengage()
    }

  def engaged: Boolean = _engaged

  def run [A] (cb: Callback [A]) (task: => Async [A]): Option [Runnable] =
    Some (new Runnable {
      def run(): Unit = Async.guard (task) ensure (reengage()) run (cb)
    })

  def launch(): Unit =
    reengage()

  def execute (f: => Any): Unit =
    fiber.execute {
      f
      if (!_engaged)
        _reengage()
    }

  def async [A] (f: Callback [A] => Any): Async [A] =
    fiber.async  { cb =>
      f (cb)
      if (!_engaged)
        _reengage()
    }}

object AsyncQueue {

  def apply (fiber: Fiber) (deque: => Option [Runnable]): AsyncQueue =
    new AsyncQueue (fiber) (deque)
}
