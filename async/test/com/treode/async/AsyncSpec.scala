package com.treode.async

import scala.util.{Failure, Success, Try}
import org.scalatest.FlatSpec

import Async.{async, guard, supply, when}
import AsyncConversions._
import AsyncTestTools._
import Callback.{ignore => disregard}

class AsyncSpec extends FlatSpec {

  class DistinguishedException extends Exception

  def exceptional [A]: Callback [A] =
    new Callback [A] {
      private var ran = false
      def apply (v: Try [A]) {
        require (!ran, "Callback was already invoked.")
        ran = true
        v match {
          case Success (v) => throw new DistinguishedException
          case Failure (t) => throw t
        }}}

  "Async.map" should "apply the function" in {
    implicit val scheduler = StubScheduler.random()
    async [Int] (_.pass (1)) .map (_ * 2) .expect (2)
  }

  it should "skip the function on a failure thrown" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    async [Int] (_ => throw new DistinguishedException)
        .map (_ => flag = true)
        .fail [DistinguishedException]
    assert (!flag)
  }

  it should "skip the function on a failure given" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    async [Int] (_.fail (new DistinguishedException))
        .map (_ => flag = true)
        .fail [DistinguishedException]
    assert (!flag)
  }

  it should "pass an exception thrown from the function to the callback" in {
    implicit val scheduler = StubScheduler.random()
    async [Int] (_.pass (1))
        .on (scheduler)
        .map (_ => throw new DistinguishedException)
        .fail [DistinguishedException]
  }

  it should "throw an exception thrown from the callback" in {
    implicit val scheduler = StubScheduler.random()
    intercept [DistinguishedException] {
      async [Int] (_.pass (1)) .map (_ * 2) .run (exceptional)
    }}

  "Async.flatMap" should "apply the function" in {
    implicit val scheduler = StubScheduler.random()
    async [Int] (_.pass (1)) .flatMap (i => supply (i * 2)) .expect (2)
  }

  it should "skip the function on a failure thrown" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    async [Int] (_ => throw new DistinguishedException)
        .flatMap (_ => supply (flag = true))
        .fail [DistinguishedException]
    assert (!flag)
  }

  it should "skip the function on a failure given" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    async [Int] (_.fail (new DistinguishedException))
        .flatMap (_ => supply (flag = true))
        .fail [DistinguishedException]
    assert (!flag)
  }

  it should "pass an exception thrown from the function to the callback" in {
    implicit val scheduler = StubScheduler.random()
    async [Int] (_.pass (1))
        .on (scheduler)
        .flatMap (_ => throw new DistinguishedException)
        .fail [DistinguishedException]
  }

  it should "pass an exception returned from the function to the callback" in {
    implicit val scheduler = StubScheduler.random()
    async [Int] (_.pass (1))
        .on (scheduler)
        .flatMap (_ => guard [Int] (throw new DistinguishedException))
        .fail [DistinguishedException]
  }

  it should "throw an exception thrown from the callback" in {
    implicit val scheduler = StubScheduler.random()
    intercept [DistinguishedException] {
      async [Int] (_.pass (1)) .flatMap (i => supply (i * 2)) .run (exceptional)
    }}

  "Async.filter" should "continue when the predicate is true" in {
    implicit val scheduler = StubScheduler.random()
    async [Int] (_.pass (1)) .filter (_ == 1) .expect (1)
  }

  it should "stop when the predicate is false" in {
    implicit val scheduler = StubScheduler.random()
    val cb = async [Int] (_.pass (1)) .filter (_ != 1) .capture()
    scheduler.runTasks()
    cb.assertNotInvoked()
  }

  it should "stop when an exeption is given" in {
    implicit val scheduler = StubScheduler.random()
    async [Int] (_.fail (new DistinguishedException))
        .filter (_ == 1)
        .fail [DistinguishedException]
  }

  it should "pass an exception thrown from the predicate to the callback" in {
    implicit val scheduler = StubScheduler.random()
    async [Int] (_.pass (1))
        .filter (_ => throw new DistinguishedException)
        .fail [DistinguishedException]
  }

  it should "throw an exception thrown from the callback" in {
    implicit val scheduler = StubScheduler.random()
    intercept [DistinguishedException] {
      async [Int] (_.pass (1)) .filter (_ == 1) .run (exceptional)
    }}

  "Async.defer" should "not invoke the callback" in {
    val cb = CallbackCaptor [Unit]
    var flag = false
    supply (flag = true) defer (cb)
    cb.assertNotInvoked()
    assertResult (true) (flag)
  }

  it should "report an exception through the callback" in {
    val cb = CallbackCaptor [Unit]
    guard (throw new DistinguishedException) defer (cb)
    cb.failed [DistinguishedException]
  }

  "Async.leave" should "invoke the body on pass" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    val a = supply (0) .leave (flag = true)
    a.pass
    assertResult (true) (flag)
  }

  it should "invoke the body on fail" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    val a = guard [Int] (throw new DistinguishedException) .leave (flag = true)
    a.fail [DistinguishedException]
    assertResult (true) (flag)
  }

  "Async.recover" should "ignore the body on pass" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    val a = supply () .recover {
      case _ => flag = true
    }
    a.pass
    assertResult (false) (flag)
  }

  it should "use the result of the body on fail" in {
    implicit val scheduler = StubScheduler.random()
    val a = guard [Int] (throw new Exception) .recover {
      case _ => 1
    }
    a.expect (1)
  }

  it should "report an exception thrown from the body through the callback" in {
    implicit val scheduler = StubScheduler.random()
    val a = guard [Int] (throw new Exception) .recover {
      case _ => throw new DistinguishedException
    }
    a.fail [DistinguishedException]
  }

  "Async.rescue" should "ignore the body on pass" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    val a = supply () .rescue {
      case _ => Try (flag = true)
    }
    a.pass
    assertResult (false) (flag)
  }

  it should "use the result of the body on fail" in {
    implicit val scheduler = StubScheduler.random()
    val a = guard [Int] (throw new Exception) .rescue {
      case _ => Try (1)
    }
    a.expect (1)
  }

  it should "report an exception thrown from the body through the callback" in {
    implicit val scheduler = StubScheduler.random()
    val a = guard [Int] (throw new Exception) .rescue {
      case _ => throw new DistinguishedException
    }
    a.fail [DistinguishedException]
  }

  it should "report an exception returned from the body through the callback" in {
    implicit val scheduler = StubScheduler.random()
    val a = guard [Int] (throw new Exception) .rescue {
      case _ => Failure (new DistinguishedException)
    }
    a.fail [DistinguishedException]
  }

  "Async.await" should "invoke the async" in {
    var flag = false
    supply (flag = true) .await()
    assertResult (true) (flag)
  }

  it should "should pass an async from the body to the caller" in {
    assertResult (1) (supply (1) .await())
  }

  it should "thrown an exception from the body to the caller" in {
    intercept [DistinguishedException] {
      supply (throw new DistinguishedException) .await()
    }}

  "Async.async" should "inovke the body" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    async [Unit] { cb => flag = true; cb.pass() } .pass
    assertResult (true) (flag)
  }

  it should "pass an exception from the body to the callback" in {
    implicit val scheduler = StubScheduler.random()
    async [Unit] (_ => throw new DistinguishedException) .fail [DistinguishedException]
  }

  it should "throw an exception from the callback" in {
    implicit val scheduler = StubScheduler.random()
    intercept [DistinguishedException] {
      async [Unit] (_.pass()) run (exceptional)
    }}

  "Async.guard" should "invoke the body" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    guard (supply (flag = true)) .pass
    assertResult (true) (flag)
  }

  it should "should pass an async from the body to the callback" in {
    implicit val scheduler = StubScheduler.random()
    assertResult (1) (guard (supply (1)) .pass)
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

  "Async.supply" should "invoke the body" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    supply (supply (flag = true)) .pass
    assertResult (true) (flag)
  }

  it should "should pass data from the body to the callback" in {
    implicit val scheduler = StubScheduler.random()
    assertResult (1) (supply (1) .pass)
  }

  it should "pass an exception from the body to the callback" in {
    implicit val scheduler = StubScheduler.random()
    supply (throw new DistinguishedException) .fail [DistinguishedException]
  }

  it should "throw an exception from the callback" in {
    implicit val scheduler = StubScheduler.random()
    intercept [DistinguishedException] {
      supply (supply()) .run (exceptional)
    }}

  "Async.when" should "invoke the body when the condition is true" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    when (true) (supply (flag = true)) .pass
    assertResult (true) (flag)
  }

  it should "skip the body when the condition is false" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    when (false) (supply (flag = true)) .pass
    assertResult (false) (flag)
  }

  it should "pass an exception from the body to the callback" in {
    implicit val scheduler = StubScheduler.random()
    when (true) (throw new DistinguishedException) .fail [DistinguishedException]
  }

  it should "throw an exception from the callback when the condition is true" in {
    implicit val scheduler = StubScheduler.random()
    intercept [DistinguishedException] {
      when (true) (supply()) .run (exceptional)
    }}

  it should "throw an exception from the callback when the condition is false" in {
    implicit val scheduler = StubScheduler.random()
    intercept [DistinguishedException] {
      when (false) (supply()) .run (exceptional)
    }}}
