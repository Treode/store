package com.treode.async

import org.scalatest.FlatSpec

import Async.{async, cond, guard, supply, whilst}
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

  def exceptional [A]: Callback [A] =
    new Callback [A] {
      def pass (v: A) = throw new DistinguishedException
      def fail (t: Throwable) = throw t
    }

  "Async.map" should "apply the function" in {
    implicit val scheduler = StubScheduler.random()
    expectPass (2) (async [Int] (_.pass (1)) .map (_ * 2))
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
    expectPass (1) (async [Int] (_.pass (1)) .filter (_ == 1))
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

  it should "throw and exception from the callback" in {
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

  it should "throw and exception from the callback when the condition is true" in {
    implicit val scheduler = StubScheduler.random()
    intercept [DistinguishedException] {
      cond (true) (supply()) .run (exceptional)
    }}

  it should "throw and exception from the callback when the condition is false" in {
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
    }}
  "Async.whilst" should "handle zero iterations" in {
    implicit val scheduler = StubScheduler.random()
    var count = 0
    whilst.f (false) (count += 1) .pass
    expectResult (0) (count)
  }

  it should "handle one iteration" in {
    implicit val scheduler = StubScheduler.random()
    var count = 0
    whilst.f (count < 1) (count += 1) .pass
    expectResult (1) (count)
  }

  it should "handle multiple iterations" in {
    implicit val scheduler = StubScheduler.random()
    var count = 0
    whilst.f (count < 3) (count += 1) .pass
    expectResult (3) (count)
  }

  it should "handle pass an exception from the body to the callback" in {
    implicit val scheduler = StubScheduler.random()
    var count = 0
    whilst.f (true) {
      count += 1
      if (count == 3)
        throw new DistinguishedException
    } .fail [DistinguishedException]
    expectResult (3) (count)
  }

  it should "handle pass an exception from the conditions to the callback" in {
    implicit val scheduler = StubScheduler.random()
    var count = 0
    whilst.f {
      if (count == 3)
        throw new DistinguishedException
      true
    } (count += 1) .fail [DistinguishedException]
    expectResult (3) (count)
  }

  it should "prevent double runs with zero iterations" in {
    implicit val scheduler = StubScheduler.random()
    val a = whilst.f (false) (())
    a run disregard
    intercept [IllegalArgumentException] {
      a run disregard
    }}

  it should "prevent double runs with multiple iterations" in {
    implicit val scheduler = StubScheduler.random()
    var count = 0
    val a = whilst.f (count < 3) (count += 1)
    a run disregard
    intercept [IllegalArgumentException] {
      a run disregard
    }}}
