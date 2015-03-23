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

import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import org.scalatest.FlatSpec

import Async.supply
import Callback.{ignore => disreguard}

class FiberSpec extends FlatSpec {

  class DistinguishedException extends Exception

  val throwDistinguishedException =
    new Runnable {
      def run() = throw new DistinguishedException
    }

  "A Fiber" should "run one task" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    var a = false
    f.execute (a = true)
    assertResult (false) (a)
    s.run()
    assertResult (true) (a)
  }

  it should "run two queued tasks" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    var a = false
    var b = false
    f.execute (a = true)
    f.execute (b = true)
    assertResult (false) (a)
    assertResult (false) (b)
    s.run()
    assertResult (true) (a)
    assertResult (true) (b)
  }

  it should "run two tasks one after the other" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    var a = false
    var b = false
    f.execute (a = true)
    assertResult (false) (a)
    s.run()
    assertResult (true) (a)
    f.execute (b = true)
    assertResult (false) (b)
    s.run()
    assertResult (true) (b)
  }

  it should "handle the return keyword" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    var b = false
    def method(): Unit = f.execute { return; b = true }
    method()
    s.run()
    assertResult (false) (b)
  }

  it should "report an exception thrown from a task and continue" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    var a = false
    f.execute (throwDistinguishedException)
    f.execute (a = true)
    assertResult (false) (a)
    intercept [DistinguishedException] (s.run())
    s.run()
    assertResult (true) (a)
  }

  "Fiber.async" should "not invoke the callback" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    var a = false
    val cb = f.async [Unit] (cb => a = true) .capture()
    assertResult (false) (a)
    cb.assertNotInvoked()
    s.run()
    assertResult (true) (a)
    cb.assertNotInvoked()
  }

  it should "reject the return keyword" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    def method(): Async [Int] = f.async (_ => return null)
    method() .fail [ReturnException]
  }

  it should "report an exception through the callback" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    f.async [Unit] (cb => throw new DistinguishedException) .fail [DistinguishedException]
  }

  "Fiber.supply" should "invoke the callback" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    var a = false
    f.supply (a = true) .expectPass()
    assertResult (true) (a)
  }

  it should "reject the return keyword" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    def method(): Async [Int] = f.supply {return null}
    method() .fail [ReturnException]
  }

  it should "report an exception through the callback" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    f.supply (throw new DistinguishedException) .fail [DistinguishedException]
  }}
