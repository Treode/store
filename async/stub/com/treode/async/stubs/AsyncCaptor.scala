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

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.implicits._

import Async.async
import Callback.fanout

/** Capture an asynchronous call so it may be completed later.
  *
  * You may `start` an asynchronous operation once; if you are mocking a method that is called
  * multiple times, return a new `AsyncCaptor` for each call.  You may not call `pass` or `fail`
  * until after  `start`.
  *
  * This class is most easily used with the single-threaded
  * [[com.treode.async.stubs.StubScheduler StubScheduler]].  If you are using a multithreaded
  * scheduler, you must arrange that `start` happens-before `pass` or `fail`.
  */
class AsyncCaptor [A] private (implicit scheduler: Scheduler) {

  private var cbs = List.empty [Callback [A]]

  def start(): Async [A] =
    async { cb =>
      require (cbs.isEmpty, "An async is already outstanding.")
      cbs ::= cb
    }

  def add(): Async [A] =
    async { cb =>
      cbs ::= cb
    }

  def pass (v: A) {
    require (!cbs.isEmpty)
    val cb = fanout (cbs)
    cbs = List.empty
    cb.pass (v)
  }

  def fail (t: Throwable) {
    require (!cbs.isEmpty)
    val cb = fanout (cbs)
    cbs = List.empty
    cb.fail (t)
  }}

object AsyncCaptor {

  def apply [A] (implicit scheduler: Scheduler): AsyncCaptor [A] =
    new AsyncCaptor [A]
}
