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
import org.scalatest.FreeSpec

import Async.{guard, supply, when}
import AsyncIteratorTestTools._

class AsyncIteratorSpec extends FreeSpec {

  "foreach should" - {

    "work with the for keyword" in {
      val xs = Seq.newBuilder [Int]
      implicit val scheduler = StubScheduler.random()
      val task = for (x <- flatten (items)) supply (xs += x)
      task.expectPass()
      assertResult (items.flatten) (xs.result)
    }

    "handle various batches" in {

      def test (items: Seq [Seq [Int]]) {
        implicit val scheduler = StubScheduler.random()
        val builder = Seq.newBuilder [Int]
        flatten (items) .foreach (x => supply (builder += x)) .expectPass()
        assertResult (items.flatten) (builder.result)
      }

      forAll (test)
    }

    "pass through an exception" in {
      implicit val scheduler = StubScheduler.random()
      guard {
        for (x <- flatten (items))
          when (x == 3) (throw new DistinguishedException)
      } .fail [DistinguishedException]
    }}

  "map should" - {

    "work with the for keyword" in {
      implicit val scheduler = StubScheduler.random()
      val iter = for (x <- flatten (items)) yield x * 2
      assertSeq (items.flatten.map (_ * 2)) (iter)
    }}


  "flatMap should" - {

    val inner = Seq (1, 2, 3)

    "work with the for keyword" in {
      implicit val scheduler = StubScheduler.random()
      val iter = for (x <- flatten (items); y <- flatten (Seq (inner))) yield (x, y)
      val expected = for (x <- items.flatten; y <- inner) yield (x, y)
      assertSeq (expected) (iter)
    }}

  "filter should" - {

    "work with the for keyword" in {
      implicit val scheduler = StubScheduler.random()
      val iter = for (x <- flatten (items); if isOdd (x)) yield x
      assertSeq (items.flatten.filter (isOdd _)) (iter)
    }}

  "whilst should" - {

    def whilst (xs: Seq [Seq [Int]]) (p: Int => Boolean) (f: Int => Any) (implicit s: StubScheduler) =
      flatten (xs) .whilst (p) (x => supply (f (x)))

    "handle an empty input iterator" in {
      implicit val scheduler = StubScheduler.random()
      var seen = Set.empty [Int]
      val last = whilst (Seq.empty) (_ => true) (seen += _) .expectPass()
      assertResult (Set.empty) (seen)
      assertResult (None) (last)
    }

    "stop when the condition fails immediately" in {
      implicit val scheduler = StubScheduler.random()
      var seen = Set.empty [Int]
      val last = whilst (items) (_ < 0) (seen += _) .expectPass()
      assertResult (Set.empty) (seen)
      assertResult (Some (1)) (last)
    }

    "stop when the condition fails before the iterator stops" in {
      implicit val scheduler = StubScheduler.random()
      var seen = Set.empty [Int]
      val last = whilst (items) (_ < 3) (seen += _) .expectPass()
      assertResult (Set (1, 2)) (seen)
      assertResult (Some (3)) (last)
    }

    "stop when the input iterator stops before the condition fails" in {
      implicit val scheduler = StubScheduler.random()
      var seen = Set.empty [Int]
      val last = whilst (items) (_ < 6) (seen += _) .expectPass()
      assertResult (Set (1, 2, 3, 4, 5)) (seen)
      assertResult (None) (last)
    }

    "pass througn an exception from the input iterator" in {
      implicit val scheduler = StubScheduler.random()
      var seen = Set.empty [Int]
      val iter =
        for (x <- flatten (items)) yield
          if (x == 3)
            throw new DistinguishedException
          else
            x
      iter.whilst (_ < 5) (x => supply (seen += x)) .fail [DistinguishedException]
      assertResult (Set.empty) (seen)
    }

    "pass througn an exception from the predicate" in {
      implicit val scheduler = StubScheduler.random()
      var seen = Set.empty [Int]
      whilst (items) { x =>
        if (x == 3)
          throw new DistinguishedException
        x < 5
      } { x =>
        supply (seen += x)
      } .fail [DistinguishedException]
      assertResult (Set (1, 2)) (seen)
    }

    "pass through an exception from the body" in {
      implicit val scheduler = StubScheduler.random()
      var seen = Set.empty [Int]
      whilst (items) (_ < 5) { x =>
        if (x == 3) throw new DistinguishedException
        supply (seen += x)
      } .fail [DistinguishedException]
      assertResult (Set (1, 2)) (seen)
    }}}
