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

package com.treode.twitter.util

import com.treode.async.Async, Async.supply
import com.twitter.util.Await, Await.result
import org.scalatest.FlatSpec

class TwitterRichAsyncSpec extends FlatSpec {

  class DistinguishedException extends Exception

  "toTwitterFuture" should "propagate success" in {
    assertResult (0) {
      result (supply [Int] (0) .toTwitterFuture)
    }}

  it should "propagate failure" in {
    intercept [DistinguishedException] {
      result (supply [Int] (throw new DistinguishedException) .toTwitterFuture)
    }}}
