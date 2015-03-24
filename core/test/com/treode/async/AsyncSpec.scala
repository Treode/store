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

import java.util.concurrent.ExecutionException
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import org.scalatest.FlatSpec

import com.treode.async.implicits._
import com.treode.async.stubs.{CallbackCaptor, StubScheduler}
import com.treode.async.stubs.implicits._

import Async.{async, guard, supply, when}
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
    async [Int] (_.pass (1)) .map (_ * 2) .expectPass (2)
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
    intercept [DistinguishedException] {
      async [Int] (_.pass (1)) .map (_ * 2) .run (exceptional)
    }}

  "Async.flatMap" should "apply the function" in {
    implicit val scheduler = StubScheduler.random()
    async [Int] (_.pass (1)) .flatMap (i => supply (i * 2)) .expectPass (2)
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
    intercept [DistinguishedException] {
      async [Int] (_.pass (1)) .flatMap (i => supply (i * 2)) .run (exceptional)
    }}

  "Async.filter" should "continue when the predicate is true" in {
    implicit val scheduler = StubScheduler.random()
    async [Int] (_.pass (1)) .filter (_ == 1) .expectPass (1)
  }

  it should "stop when the predicate is false" in {
    implicit val scheduler = StubScheduler.random()
    async [Int] (_.pass (1))
      .filter (_ != 1)
      .fail [NoSuchElementException]
  }

  it should "stop when an exception is given" in {
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
    intercept [DistinguishedException] {
      async [Int] (_.pass (1)) .filter (_ == 1) .run (exceptional)
    }}

  "Async.ensure" should "invoke the body on pass" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    val a = supply (0) .ensure (flag = true)
    a.expectPass()
    assertResult (true) (flag)
  }

  it should "invoke the body on fail" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    val a = guard [Int] (throw new DistinguishedException) .ensure (flag = true)
    a.fail [DistinguishedException]
    assertResult (true) (flag)
  }

  "Async.recover" should "ignore the body on pass" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    val a = supply (()) .recover {
      case _ => flag = true
    }
    a.expectPass()
    assertResult (false) (flag)
  }

  it should "use the result of the body on fail" in {
    implicit val scheduler = StubScheduler.random()
    val a = guard [Int] (throw new Exception) .recover {
      case _ => 1
    }
    a.expectPass (1)
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
    val a = supply (()) .rescue {
      case _ => Try (flag = true)
    }
    a.expectPass()
    assertResult (false) (flag)
  }

  it should "use the result of the body on fail" in {
    implicit val scheduler = StubScheduler.random()
    val a = guard [Int] (throw new Exception) .rescue {
      case _ => Try (1)
    }
    a.expectPass (1)
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

  it should "throw an exception from the body to the caller" in {
    intercept [DistinguishedException] {
      supply (throw new DistinguishedException) .await()
    }}

  "Async.toGuavaFuture" should "propagate success" in {
    assertResult (0) {
      supply [Int] (0) .toGuavaFuture.get
    }}

  it should "propagate failure" in {
    val t = intercept [ExecutionException] {
      supply [Int] (throw new DistinguishedException) .toGuavaFuture.get
    }
    assert (t.getCause.isInstanceOf [DistinguishedException])
  }

  "Async.toJavaFuture" should "propagate success" in {
    assertResult (0) {
      supply [Int] (0) .toJavaFuture.get
    }}

  it should "propagate failure" in {
    val t = intercept [ExecutionException] {
      supply [Int] (throw new DistinguishedException) .toJavaFuture.get
    }
    assert (t.getCause.isInstanceOf [DistinguishedException])
  }

  "Async.toScalaFuture" should "propagate success" in {
    assertResult (0) {
      Await.result (supply [Int] (0) .toScalaFuture, Duration.Inf)
    }}

  it should "propagate failure" in {
    intercept [DistinguishedException] {
      Await.result (supply [Int] (throw new DistinguishedException) .toScalaFuture, Duration.Inf)
    }}

  "Async.async" should "inovke the body" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    async [Unit] { cb => flag = true; cb.pass (()) } .expectPass()
    assertResult (true) (flag)
  }

  it should "reject the return keyword" in {
    implicit val scheduler = StubScheduler.random()
    def method(): Async [Int] = async (_ => return null)
    method() .fail [ReturnException]
  }

  it should "pass an exception from the body to the callback" in {
    implicit val scheduler = StubScheduler.random()
    async [Unit] (_ => throw new DistinguishedException) .fail [DistinguishedException]
  }

  it should "throw an exception from the callback" in {
    intercept [DistinguishedException] {
      async [Unit] (_.pass (())) run (exceptional)
    }}

  it should "allow storing the callback and passing it later" in {
    var callback: Callback [Int] = null
    val captor = async [Int] (callback = _) .capture()
    callback.pass (1)
    captor.assertPassed (1)
  }

  it should "allow storing the callback and failing it later" in {
    var callback: Callback [Int] = null
    val captor = async [Int] (callback = _) .capture()
    callback.fail (new DistinguishedException)
    captor.assertFailed [DistinguishedException]
  }

  it should "handle storing the callback that throws an exception later" in {
    var callback: Callback [Int] = null
    async [Int] (callback = _) run (_ => throw new DistinguishedException)
    intercept [CallbackException] {
      callback.pass (1)
    }}

  "Async.guard" should "invoke the body" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    guard (supply (flag = true)) .expectPass()
    assertResult (true) (flag)
  }

  it should "return the return keyword" in {
    implicit val scheduler = StubScheduler.random()
    def method(): Async [Int] = guard (return supply (0))
    method() .fail [ReturnException]
  }

  it should "should pass an async from the body to the callback" in {
    implicit val scheduler = StubScheduler.random()
    assertResult (1) (guard (supply (1)) .expectPass())
  }

  it should "pass an exception from the body to the callback" in {
    implicit val scheduler = StubScheduler.random()
    guard (throw new DistinguishedException) .fail [DistinguishedException]
  }

  it should "throw an exception from the callback" in {
    intercept [DistinguishedException] {
      guard (supply (())) .run (exceptional)
    }}

  "Async.supply" should "invoke the body" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    supply (supply (flag = true)) .expectPass()
    assertResult (true) (flag)
  }

  it should "reject the return keyword" in {
    implicit val scheduler = StubScheduler.random()
    def method(): Async [Int] = supply {return null}
    method() .fail [ReturnException]
  }

  it should "should pass data from the body to the callback" in {
    implicit val scheduler = StubScheduler.random()
    assertResult (1) (supply (1) .expectPass())
  }

  it should "pass an exception from the body to the callback" in {
    implicit val scheduler = StubScheduler.random()
    supply (throw new DistinguishedException) .fail [DistinguishedException]
  }

  it should "throw an exception from the callback" in {
    intercept [DistinguishedException] {
      supply (supply (())) .run (exceptional)
    }}

  "Async.when on a predicate" should "invoke the body when the condition is true" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    when (true) (supply (flag = true)) .expectPass()
    assertResult (true) (flag)
  }

  it should "skip the body when the condition is false" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    when (false) (supply (flag = true)) .expectPass()
    assertResult (false) (flag)
  }

  it should "pass an exception from the body to the callback" in {
    implicit val scheduler = StubScheduler.random()
    when (true) (throw new DistinguishedException) .fail [DistinguishedException]
  }

  it should "throw an exception from the callback when the condition is true" in {
    intercept [DistinguishedException] {
      when (true) (supply (())) .run (exceptional)
    }}

  it should "throw an exception from the callback when the condition is false" in {
    intercept [DistinguishedException] {
      when (false) (supply (())) .run (exceptional)
    }}

  "Async.when on an option" should "invoke the body when the option is defined" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    when (Some (1)) (_ => supply (flag = true)) .expectPass()
    assertResult (true) (flag)
  }

  it should "skip the body when the option is empty" in {
    implicit val scheduler = StubScheduler.random()
    var flag = false
    when (None) (_ => supply (flag = true)) .expectPass()
    assertResult (false) (flag)
  }

  it should "pass an exception from the body to the callback" in {
    implicit val scheduler = StubScheduler.random()
    when (Some (1)) (_ => throw new DistinguishedException) .fail [DistinguishedException]
  }

  it should "throw an exception from the callback when the condition is true" in {
    intercept [DistinguishedException] {
      when (None) (_ => supply (())) .run (exceptional)
    }}

  it should "throw an exception from the callback when the condition is false" in {
    intercept [DistinguishedException] {
      when (None) (_ => supply (())) .run (exceptional)
    }}}
