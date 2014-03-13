package com.treode.async

import scala.util.{Failure, Success}
import org.scalatest.FlatSpec

import Async.{async, cond, guard, supply}
import AsyncConversions._
import AsyncTestTools._
import Callback.{ignore => disregard}

class AsyncSpec extends FlatSpec {

  class DistinguishedException extends Exception

  class ScheduledAsync [A] (async: Async [A], scheduler: Scheduler) extends Async [A] {

    def run (cb: Callback [A]): Unit =
      async.run (scheduler.take (cb))
  }

  implicit class ExtraRichAsync [A] (async: Async [A]) {

    def on (scheduler: Scheduler): Async [A] =
      new ScheduledAsync (async, scheduler)
  }

  def exceptional [A]: Callback [A] = {
    case Success (v) => throw new DistinguishedException
    case Failure (t) => throw t
  }

  "Async.map" should "apply the function" in {
    implicit val scheduler = StubScheduler.random()
    async [Int] (_.pass (1)) .map (_ * 2) .expect (2)
  }

  it should "pass an exception from the function to the callback" in {
    implicit val scheduler = StubScheduler.random()
    async [Int] (_.pass (1))
        .on (scheduler)
        .map (_ => throw new DistinguishedException)
        .fail [DistinguishedException]
  }

  it should "thrown an exception from the callback" in {
    implicit val scheduler = StubScheduler.random()
    intercept [DistinguishedException] {
      async [Int] (_.pass (1)) .map (_ * 2) .run (exceptional)
    }}

  "Async.filter" should "continue when the predicate is true" in {
    implicit val scheduler = StubScheduler.random()
    async [Int] (_.pass (1)) .filter (_ == 1) .expect (1)
  }

  it should "stop when the predicate is false" in {
    implicit val scheduler = StubScheduler.random()
    val cb = async [Int] (_.pass (1)) .filter (_ != 1) .capture()
    scheduler.runTasks()
    cb.expectNotInvoked()
  }

  it should "pass an exception from the predicate to the callback" in {
    implicit val scheduler = StubScheduler.random()
    async [Int] (_.pass (1))
        .on (scheduler)
        .filter (_ => throw new DistinguishedException)
        .fail [DistinguishedException]
  }

  it should "thrown an exception from the callback" in {
    implicit val scheduler = StubScheduler.random()
    intercept [DistinguishedException] {
      async [Int] (_.pass (1)) .filter (_ == 1) .run (exceptional)
    }}

  "Async.defer" should "not invoke the callback" in {
    val cb = CallbackCaptor [Unit]
    var flag = false
    supply (flag = true) defer (cb)
    cb.expectNotInvoked()
    expectResult (true) (flag)
  }

  it should "report an exception through the callback" in {
    val cb = CallbackCaptor [Unit]
    guard (throw new DistinguishedException) defer (cb)
    cb.failed [DistinguishedException]
  }

  "Async.onLeave" should "invoke the body on pass" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    val a = supply (0) .leave (flag = true)
    a.pass
    expectResult (true) (flag)
  }

  it should "invoke the body on fail" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    val a = guard [Int] (throw new DistinguishedException) .leave (flag = true)
    a.fail [DistinguishedException]
    expectResult (true) (flag)
  }

  "Async.async" should "inovke the body" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    async [Unit] { cb => flag = true; cb.pass() } .pass
    expectResult (true) (flag)
  }

  it should "pass an exception from the body to the callback" in {
    implicit val scheduler = StubScheduler.random()
    async [Unit] (_ => throw new DistinguishedException) .fail [DistinguishedException]
  }

  it should "thrown an exception from the callback" in {
    implicit val scheduler = StubScheduler.random()
    intercept [DistinguishedException] {
      async [Unit] (_.pass()) run (exceptional)
    }}

  it should "prevent double runs" in {
    implicit val scheduler = StubScheduler.random()
    val a = async [Unit] (_.pass())
    a run disregard
    intercept [IllegalArgumentException] {
      a run disregard
    }}

  "Async.guard" should "invoke the body" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    guard (supply (flag = true)) .pass
    expectResult (true) (flag)
  }

  it should "pass an exception from the body to the callback" in {
    implicit val scheduler = StubScheduler.random()
    guard (throw new DistinguishedException) .fail [DistinguishedException]
  }

  it should "throw an exception from the callback" in {
    implicit val scheduler = StubScheduler.random()
    intercept [DistinguishedException] {
      guard (supply()) .run (exceptional)
    }}

  it should "prevent double runs" in {
    implicit val scheduler = StubScheduler.random()
    val a = guard (supply())
    a run disregard
    intercept [IllegalArgumentException] {
      a run disregard
    }}

  "Async.cond" should "invoke the body when the condition is true" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    cond (true) (supply (flag = true)) .pass
    expectResult (true) (flag)
  }

  it should "skip the body when the condition is false" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    cond (false) (supply (flag = true)) .pass
    expectResult (false) (flag)
  }

  it should "pass an exception from the body to the callback" in {
    implicit val scheduler = StubScheduler.random()
    cond (true) (throw new DistinguishedException) .fail [DistinguishedException]
  }

  it should "throw an exception from the callback when the condition is true" in {
    implicit val scheduler = StubScheduler.random()
    intercept [DistinguishedException] {
      cond (true) (supply()) .run (exceptional)
    }}

  it should "throw an exception from the callback when the condition is false" in {
    implicit val scheduler = StubScheduler.random()
    intercept [DistinguishedException] {
      cond (false) (supply()) .run (exceptional)
    }}

  it should "prevent double runs when the condition is true" in {
    implicit val scheduler = StubScheduler.random()
    val a = cond (true) (supply())
    a run disregard
    intercept [IllegalArgumentException] {
      a run disregard
    }}

  it should "prevent double runs when the condition is false" in {
    implicit val scheduler = StubScheduler.random()
    val a = cond (false) (supply())
    a run disregard
    intercept [IllegalArgumentException] {
      a run disregard
    }}}
