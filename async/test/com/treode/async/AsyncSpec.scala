package com.treode.async

import org.scalatest.FlatSpec

import Async.{async, whilst}
import AsyncTestTools._

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
      async [Unit] (_.pass ()) run (exceptional)
    }}

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
  }}
