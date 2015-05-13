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
import scala.util.{Failure, Success}

import com.treode.async.implicits._
import com.treode.async.stubs.{AsyncCaptor, CallbackCaptor, StubScheduler}
import com.treode.async.stubs.implicits._
import org.scalatest.FlatSpec

import Async.{async, supply}
import Callback.{ignore => disregard}

class AsyncQueueSpec extends FlatSpec {

  class DistinguishedException extends Exception

  def tee [A] (cb1: Callback [A]) (task: Async [A]): Async [A] =
    async { cb2 =>
      task.run {
        case Success (v) => cb1.pass (v); cb2.pass (v)
        case Failure (t) => cb1.fail (t)
      }}

  class TestQueue (implicit scheduler: StubScheduler) {

    private val fiber = new Fiber
    private val queue = new AsyncQueue (fiber) (reengage _)
    private var callbacks = new ArrayDeque [Callback [Unit]]
    private var captor = AsyncCaptor [Unit]

    // If there are outstanding user requests, start the next one.
    def reengage(): Unit =
      if (!callbacks.isEmpty) {
        queue.begin {
          val userCB = callbacks.remove()
          async [Unit] { queueCB =>
            captor.start() run {
              // Send pass/fail to user operation; pass queue operation.
              case Success (_) => userCB.pass (()); queueCB.pass (())
              case Failure (t) => userCB.fail (t); queueCB.pass (())
            }
          }
        }
      }

    // Submit a user request for another async operation.
    def start(): Async [Unit] =
      fiber.async { cb =>
        callbacks.add (cb)
        queue.engage()
      }

    // Pass the next async operation.
    def pass() {
      captor.pass (())
      scheduler.run()
    }

    // Fail the next asynch operation.
    def fail (t: Throwable) {
      captor.fail (t)
      scheduler.run()
    }}

  "An AsyncQueue" should "run one task" in {
    implicit val s = StubScheduler.random()
    val q = new TestQueue
    val cb = q.start().capture()
    s.run()
    cb.assertNotInvoked()
    q.pass()
    cb.assertPassed
  }

  it should "run two queue tasks" in {
    implicit val s = StubScheduler.random()
    val q = new TestQueue
    val cb1 = q.start().capture()
    val cb2 = q.start().capture()
    s.run()
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
    val cb1 = q.start().capture()
    s.run()
    cb1.assertNotInvoked()
    q.pass()
    cb1.assertPassed()
    val cb2 = q.start().capture()
    s.run()
    cb2.assertNotInvoked()
    q.pass()
    cb2.assertPassed()
  }

  it should "report an exception through the callback and continue" in {
    implicit val s = StubScheduler.random()
    val q = new TestQueue
    val cb1 = q.start().capture()
    val cb2 = q.start().capture()
    s.run()
    cb1.assertNotInvoked()
    cb2.assertNotInvoked()
    q.fail (new DistinguishedException)
    cb1.assertFailed [DistinguishedException]
    cb2.assertNotInvoked()
    q.pass()
    cb2.assertPassed()
  }}
