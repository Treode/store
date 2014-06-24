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

import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import org.scalatest.FlatSpec

import Async.supply
import Callback.{ignore => disregard}

class RichExecutorSpec extends FlatSpec {

  class DistinguishedException extends Exception

  "Async.whilst" should "handle zero iterations" in {
    implicit val s = StubScheduler.random()
    var count = 0
    s.whilst (false) (supply (count += 1)) .pass
    assertResult (0) (count)
  }

  it should "handle one iteration" in {
    implicit val s = StubScheduler.random()
    var count = 0
    s.whilst (count < 1) (supply (count += 1)) .pass
    assertResult (1) (count)
  }

  it should "handle multiple iterations" in {
    implicit val s = StubScheduler.random()
    var count = 0
    s.whilst (count < 3) (supply (count += 1)) .pass
    assertResult (3) (count)
  }

  it should "pass an exception from the condition to the callback" in {
    implicit val s = StubScheduler.random()
    var count = 0
    s.whilst {
      if (count == 3)
        throw new DistinguishedException
      true
    } (supply (count += 1)) .fail [DistinguishedException]
    assertResult (3) (count)
  }

    it should "pass an exception returned from the body to the callback" in {
    implicit val s = StubScheduler.random()
    var count = 0
    s.whilst (true) {
      supply {
        count += 1
        if (count == 3)
          throw new DistinguishedException
      }
    } .fail [DistinguishedException]
    assertResult (3) (count)
  }

  it should "pass an exception thrown from the body to the callback" in {
    implicit val s = StubScheduler.random()
    var count = 0
    s.whilst (true) {
      if (count == 3)
        throw new DistinguishedException
      supply (count += 1)
    } .fail [DistinguishedException]
    assertResult (3) (count)
  }}
