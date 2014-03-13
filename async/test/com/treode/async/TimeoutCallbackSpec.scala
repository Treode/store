package com.treode.async

import java.util.concurrent.TimeoutException
import scala.util.Random

import org.scalatest.FlatSpec

import AsyncConversions._

class TimeoutCallbackSpec extends FlatSpec {

  class DistinguishedException extends Exception

  val backoff = Backoff (10, 20, retries = 3)

  def setup() = {
    implicit val random = new Random (0)
    implicit val scheduler = StubScheduler.random (random)
    val fiber = new Fiber (scheduler)
    val captor = CallbackCaptor [Unit]
    (random, scheduler, fiber, captor)
  }

  "The TimeoutCallback" should "rouse only once when the work passes quickly" in {
    implicit val (random, scheduler, fiber, captor) = setup()
    var count = 0
    val timer = captor.timeout (fiber, backoff) (count += 1)
    timer.pass()
    assert (timer.invoked)
    captor.expectInvoked()
    scheduler.runTasks (timers = true)
    expectResult (1) (count)
  }

  it should "rouse only once when the work fails quickly" in {
    implicit val (random, scheduler, fiber, captor) = setup()
    var count = 0
    val timer = captor.timeout (fiber, backoff) (count += 1)
    timer.fail (new DistinguishedException)
    assert (timer.invoked)
    captor.failed [DistinguishedException]
    scheduler.runTasks (timers = true)
    expectResult (1) (count)
  }

  it should "rouse until the work passes when it does pass eventually" in {
    implicit val (random, scheduler, fiber, captor) = setup()
    var count = 0
    val timer = captor.timeout (fiber, backoff) (count += 1)
    assert (!timer.invoked)
    captor.expectNotInvoked()
    scheduler.runTasks (timers = true, count = 2)
    expectResult (2) (count)
    assert (!timer.invoked)
    captor.expectNotInvoked()
    timer.pass()
    assert (timer.invoked)
    captor.expectInvoked()
    scheduler.runTasks (timers = true)
    expectResult (2) (count)
  }

  it should "rouse until the work fails when it does fail eventually" in {
    implicit val (random, scheduler, fiber, captor) = setup()
    var count = 0
    val timer = captor.timeout (fiber, backoff) (count += 1)
    assert (!timer.invoked)
    captor.expectNotInvoked()
    scheduler.runTasks (timers = true, count = 2)
    expectResult (2) (count)
    assert (!timer.invoked)
    captor.expectNotInvoked()
    timer.fail (new DistinguishedException)
    assert (timer.invoked)
    captor.failed [DistinguishedException]
    scheduler.runTasks (timers = true)
    expectResult (2) (count)
  }

  it should "rouse until the iterator is exhaused when the work does not pass or fail" in {
    implicit val (random, scheduler, fiber, captor) = setup()
    var count = 0
    val timer = captor.timeout (fiber, backoff) (count += 1)
    assert (!timer.invoked)
    captor.expectNotInvoked()
    scheduler.runTasks (timers = true)
    expectResult (3) (count)
    assert (timer.invoked)
    captor.failed [TimeoutException]
  }

  it should "work with leave to close on pass" in {
    implicit val (random, scheduler, fiber, captor) = setup()
    var flag = false
    val timer = captor.leave (flag = true) .timeout (fiber, backoff) ()
    timer.pass()
    scheduler.runTasks (timers = true)
    assert (timer.invoked)
    captor.expectInvoked()
    assert (flag)
  }

  it should "work with leave to close on fail" in {
    implicit val (random, scheduler, fiber, captor) = setup()
    var flag = false
    val timer = captor.leave (flag = true) .timeout (fiber, backoff) ()
    timer.fail (new DistinguishedException)
    scheduler.runTasks (timers = true)
    assert (timer.invoked)
    captor.failed [DistinguishedException]
    assert (flag)
  }

  it should "work with leave to close on timeout" in {
    implicit val (random, scheduler, fiber, captor) = setup()
    var flag = false
    val timer = captor.leave (flag = true) .timeout (fiber, backoff) ()
    scheduler.runTasks (timers = true)
    assert (timer.invoked)
    captor.failed [TimeoutException]
    assert (flag)
  }}
