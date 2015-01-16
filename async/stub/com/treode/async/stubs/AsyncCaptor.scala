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

import java.util.ArrayDeque

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.implicits._

import Async.async
import Callback.fanout

/** Capture an asynchronous call so you may be completed later. */
class AsyncCaptor [A] private (implicit scheduler: StubScheduler) {

  private val cbs = new ArrayDeque [Callback [A]]

  def outstanding: Int = cbs.size

  /** Simulate a call to an asynchronous function. Returns an Async to the caller that can be
    * completed later using `pass` or `fail`. Multiple calls to start are queued (FIFO).
    */
  def start(): Async [A] =
    async { cb =>
      cbs.add (cb)
    }

  /** Pass the next asynchronous function that was started earlier. */
  def pass (v: A) {
    require (!cbs.isEmpty, "No outstanding asynchronous calls.")
    cbs.remove.pass (v)
    scheduler.run()
  }

  /** Fail the next asynchronous function that was started earlier. */
  def fail (t: Throwable) {
    require (!cbs.isEmpty, "No outstanding asynchronous calls.")
    cbs.remove.fail (t)
    scheduler.run()
  }}

object AsyncCaptor {

  def apply [A] (implicit scheduler: StubScheduler): AsyncCaptor [A] =
    new AsyncCaptor [A]
}
