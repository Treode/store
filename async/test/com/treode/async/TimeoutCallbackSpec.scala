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

import java.util.concurrent.TimeoutException
import scala.util.Random

import com.treode.async.implicits._
import com.treode.async.stubs.{CallbackCaptor, StubScheduler}
import org.scalatest.FlatSpec

class TimeoutCallbackSpec extends FlatSpec {

  class DistinguishedException extends Exception

  val backoff = Backoff (10, 20, retries = 3)

  def setup() = {
    implicit val random = new Random (0)
    implicit val scheduler = StubScheduler.random (random)
    val fiber = new Fiber
    val captor = CallbackCaptor [Unit]
    (random, scheduler, fiber, captor)
  }

  "The TimeoutCallback" should "rouse only once when the work passes quickly" in {
    implicit val (random, scheduler, fiber, captor) = setup()
    var count = 0
    val timer = captor.timeout (fiber, backoff) (count += 1)
    timer.rouse()
    timer.pass()
    scheduler.run()
    assert (timer.invoked)
    captor.assertInvoked()
    scheduler.run (timers = true)
    assertResult (1) (count)
  }

  it should "rouse only once when the work fails quickly" in {
    implicit val (random, scheduler, fiber, captor) = setup()
    var count = 0
    val timer = captor.timeout (fiber, backoff) (count += 1)
    timer.rouse()
    timer.fail (new DistinguishedException)
    scheduler.run()
    assert (timer.invoked)
    captor.failed [DistinguishedException]
    scheduler.run (timers = true)
    assertResult (1) (count)
  }

  it should "rouse until the work passes when it does pass eventually" in {
    implicit val (random, scheduler, fiber, captor) = setup()
    var count = 0
    val timer = captor.timeout (fiber, backoff) (count += 1)
    timer.rouse()
    scheduler.run()
    assert (!timer.invoked)
    captor.assertNotInvoked()
    scheduler.run (timers = count < 3)
    assert (!timer.invoked)
    captor.assertNotInvoked()
    timer.pass()
    scheduler.run()
    assert (timer.invoked)
    captor.assertInvoked()
    scheduler.run (timers = true)
    assertResult (3) (count)
  }

  it should "rouse until the work fails when it does fail eventually" in {
    implicit val (random, scheduler, fiber, captor) = setup()
    var count = 0
    val timer = captor.timeout (fiber, backoff) (count += 1)
    timer.rouse()
    scheduler.run()
    assert (!timer.invoked)
    captor.assertNotInvoked()
    scheduler.run (timers = count < 3)
    assert (!timer.invoked)
    captor.assertNotInvoked()
    timer.fail (new DistinguishedException)
    scheduler.run()
    assert (timer.invoked)
    captor.failed [DistinguishedException]
    scheduler.run (timers = true)
    assertResult (3) (count)
  }

  it should "rouse until the iterator is exhaused when the work does not pass or fail" in {
    implicit val (random, scheduler, fiber, captor) = setup()
    var count = 0
    val timer = captor.timeout (fiber, backoff) (count += 1)
    timer.rouse()
    scheduler.run()
    assert (!timer.invoked)
    captor.assertNotInvoked()
    scheduler.run (timers = true)
    assertResult (3) (count)
    assert (timer.invoked)
    captor.failed [TimeoutException]
  }

  it should "work with ensure to close on pass" in {
    implicit val (random, scheduler, fiber, captor) = setup()
    var flag = false
    val timer = captor.ensure (flag = true) .timeout (fiber, backoff) ()
    timer.rouse()
    scheduler.run()
    timer.pass()
    scheduler.run (timers = true)
    assert (timer.invoked)
    captor.assertInvoked()
    assert (flag)
  }

  it should "work with leave to close on fail" in {
    implicit val (random, scheduler, fiber, captor) = setup()
    var flag = false
    val timer = captor.ensure (flag = true) .timeout (fiber, backoff) ()
    timer.rouse()
    scheduler.run()
    timer.fail (new DistinguishedException)
    scheduler.run (timers = true)
    assert (timer.invoked)
    captor.failed [DistinguishedException]
    assert (flag)
  }

  it should "work with leave to close on timeout" in {
    implicit val (random, scheduler, fiber, captor) = setup()
    var flag = false
    val timer = captor.ensure (flag = true) .timeout (fiber, backoff) ()
    timer.rouse()
    scheduler.run (timers = true)
    assert (timer.invoked)
    captor.failed [TimeoutException]
    assert (flag)
  }}
