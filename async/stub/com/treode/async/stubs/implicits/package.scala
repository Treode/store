package com.treode.async.stubs

import com.treode.async.{Async, AsyncIterator}
import org.scalatest.Assertions

import Assertions.assertResult
import Async.supply

package object implicits {

  implicit class TestingAsync [A] (async: Async [A]) {

    /** Provide a [[com.treode.async.stubs.CallbackCaptor CallbackCaptor]] to the asynchronous
      * operation and return that captor.
      */
    def capture(): CallbackCaptor [A] = {
      val cb = CallbackCaptor [A]
      async run cb
      cb
    }

    /** Run until the asynchronous operation completes, then assert that it yielded
      * [[scala.util.Success Success]] and return the result.
      */
    def pass (implicit scheduler: StubScheduler): A = {
      val cb = capture()
      scheduler.run (!cb.wasInvoked)
      cb.passed
    }

    /** Run until the asynchronous operation completes, then assert that it yielded
      * [[scala.util.Failure Failure]] and return the exception.
      */
    def fail [E] (implicit scheduler: StubScheduler, m: Manifest [E]): E = {
      val cb = capture()
      scheduler.run (!cb.wasInvoked)
      cb.failed [E]
    }

    /** Run until the asynchronous operation completes, then assert that it yielded
      * [[scala.util.Success Success]] and check that the result is as expected.
      */
    def expect (expected: A) (implicit scheduler: StubScheduler): Unit =
      assertResult (expected) (pass)

    /** Run until the asynchronous operation completes, then assert that it yielded
      * [[scala.util.Success Success]] and check that the
      * [[scala.collection.Seq Seq]] result is as expected.
      */
    def expectSeq [B] (xs: B*) (implicit s: StubScheduler, w: A <:< Seq [B]): Unit =
      assertResult (xs) (pass)
  }}
