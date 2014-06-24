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

import scala.util.Random
import org.scalacheck.Gen
import org.scalatest.FlatSpec
import org.scalatest.prop.PropertyChecks

import PropertyChecks._

class BackoffSpec extends FlatSpec {

  val seeds = Gen.choose (0L, Long.MaxValue)
  val retries = Gen.choose (1, 37)
  val max = Gen.choose (100, 1000)

  "Backoff" should "provide limited retries" in {
    forAll (seeds, retries) { case (seed, retries) =>
      implicit val random = new Random (seed)
      val backoff = Backoff (30, 20, 400, retries)
      assertResult (retries) (backoff.iterator.length)
    }}

  it should "usually grow" in {
    forAll (seeds, max) { case (seed, max) =>
      implicit val random = new Random (seed)
      val backoff = Backoff (30, 20, max, 37)
      var prev = 0
      var count = 0
      for (i <- backoff.iterator) {
        if (i == prev)
          count += 1
        assert (i > prev || i == max || count < 3)
        prev = i
      }}}

  it should "not exceed the maximum" in {
    forAll (seeds, max) { case (seed, max) =>
      implicit val random = new Random (seed)
      val backoff = Backoff (30, 20, max, 37)
      for (i <- backoff.iterator)
        assert (i <= max)
    }}}
