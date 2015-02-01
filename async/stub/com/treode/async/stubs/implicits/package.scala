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

package com.treode.async.stubs

import com.treode.async.Async
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
    def expectPass () (implicit scheduler: StubScheduler): A =
      capture() .expectPass()

    /** Run until the asynchronous operation completes, then assert that it yielded
      * [[scala.util.Success Failure]] and assert that the result is as expected.
      */
    def expectPass (expected: Any) (implicit scheduler: StubScheduler): Unit =
      assertResult (expected) (expectPass())

    /** Run until the asynchronous operation completes, then assert that it yielded
      * [[scala.util.Success Success]] and assert that the
      * [[scala.collection.Seq Seq]] result is as expected.
      */
    def expectSeq [B] (xs: B*) (implicit s: StubScheduler, w: A <:< Seq [B]): Unit =
      capture() .expectSeq (xs: _*)

    // TODO: rename to expectFail
    /** Run until the asynchronous operation completes, then assert that it yielded
      * [[scala.util.Failure Failure]] and return the exception.
      */
    def fail [E] (implicit scheduler: StubScheduler, m: Manifest [E]): E =
      capture() .expectFail [E]
  }}
