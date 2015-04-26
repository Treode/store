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

import java.util.ArrayDeque

import com.treode.async.stubs.{AsyncCaptor, CallbackCaptor, StubScheduler}
import com.treode.async.stubs.implicits._
import org.scalatest.FlatSpec

import Async.{async, supply}
import Callback.{ignore => disregard}

class AsyncQueueSpec extends FlatSpec {

  class DistinguishedException extends Exception

  class TestQueue (implicit scheduler: StubScheduler) {

    val fiber = new Fiber
    val queue = new AsyncQueue (fiber) (reengage _)
    var callbacks = new ArrayDeque [Callback [Unit]]
    var captor = AsyncCaptor [Unit]

    queue.launch()

    def reengage(): Unit =
      if (!callbacks.isEmpty)
        queue.run (callbacks.remove()) (captor.start())

    def start(): CallbackCaptor [Unit] = {
      val cb = queue.async [Unit] (cb => callbacks.add (cb)) .capture()
      scheduler.run()
      cb
    }

    def pass() {
      captor.pass (())
      scheduler.run()
    }

    def fail (t: Throwable) {
      captor.fail (t)
      scheduler.run()
    }}

  "An AsyncQueue" should "run one task" in {
    implicit val s = StubScheduler.random()
    val q = new TestQueue
    val cb = q.start()
    cb.assertNotInvoked()
    q.pass()
    cb.assertPassed
  }

  it should "run two queue tasks" in {
    implicit val s = StubScheduler.random()
    val q = new TestQueue
    val cb1 = q.start()
    val cb2 = q.start()
    cb1.assertNotInvoked()
    cb2.assertNotInvoked()
    q.pass()
    cb1.assertPassed()
    cb2.assertNotInvoked()
    q.pass()
    cb2.assertPassed()
  }

  it should "run two tasks one after the other" in {
    implicit val s = StubScheduler.random()
    val q = new TestQueue
    val cb1 = q.start()
    cb1.assertNotInvoked()
    q.pass()
    cb1.assertPassed()
    val cb2 = q.start()
    cb2.assertNotInvoked()
    q.pass()
    cb2.assertPassed()
  }

  it should "report an exception through the callback and continue" in {
    implicit val s = StubScheduler.random()
    val q = new TestQueue
    val cb1 = q.start()
    val cb2 = q.start()
    cb1.assertNotInvoked()
    cb2.assertNotInvoked()
    q.fail (new DistinguishedException)
    cb1.assertFailed [DistinguishedException]
    cb2.assertNotInvoked()
    q.pass()
    cb2.assertPassed()
  }}
