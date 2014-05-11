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

    /** Run all tasks on the scheduler until none remain, then assert that the asynchronous
      * operation completed with [[scala.util.Success Success]] and return the result.
      */
    def pass (implicit scheduler: StubScheduler): A = {
      val cb = capture()
      scheduler.run()
      cb.passed
    }

    /** Run all tasks on the scheduler until none remain, then assert that the asynchronous
      * operation completed with [[scala.util.Failure Failure]] and return the exception.
      */
    def fail [E] (implicit scheduler: StubScheduler, m: Manifest [E]): E = {
      val cb = capture()
      scheduler.run()
      cb.failed [E]
    }

    /** Run all tasks on the scheduler until none remain, then assert that the asynchronous
      * operation completed with [[scala.util.Success Success]] and check that the result is
      * as expected.
      */
    def expect (expected: A) (implicit scheduler: StubScheduler): Unit =
      assertResult (expected) (pass)

    /** Run all tasks on the scheduler until none remain, then assert that the asynchronous
      * operation completed with [[scala.util.Success Success]] and check that the
      * [[scala.collection.Seq Seq]] result is as expected.
      */
    def expectSeq [B] (xs: B*) (implicit s: StubScheduler, w: A <:< Seq [B]): Unit =
      assertResult (xs) (pass)
  }

  implicit class TestingAsyncIterator [A] (iter: AsyncIterator [A]) {

    /** Iterate the entire asynchronous iterator and return a standard sequence. */
    def toSeq (implicit scheduler: StubScheduler): Seq [A] = {
      val builder = Seq.newBuilder [A]
      iter.foreach (x => supply (builder += x)) .pass
      builder.result
    }}}
