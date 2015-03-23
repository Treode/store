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

class BatchIteratorSpec extends FreeSpec {

  "foreach should" - {

    "work with the for keyword" in {
      val xs = Seq.newBuilder [Int]
      implicit val scheduler = StubScheduler.random()
      val task = for (x <- batch (items)) xs += x
      task.expectPass()
      assertResult (items.flatten) (xs.result)
    }

    "handle various batches" in {

      def test (items: Seq [Seq [Int]]) {
        implicit val scheduler = StubScheduler.random()
        val builder = Seq.newBuilder [Int]
        batch (items) .foreach (builder += _) .expectPass()
        assertResult (items.flatten) (builder.result)
      }

      forAll (test)
    }

    "pass through an exception" in {
      implicit val scheduler = StubScheduler.random()
      guard {
        for (x <- batch (items))
          if (x == 3)
            throw new DistinguishedException
      } .fail [DistinguishedException]
    }}

  "map should" - {

    "work with the for keyword" in {
      implicit val scheduler = StubScheduler.random()
      val iter = for (x <- batch (items)) yield x * 2
      assertSeq (items.flatten.map (_ * 2)) (iter)
    }

    "handle various batches" in {

      def test (items: Seq [Seq [Int]]) {
        implicit val scheduler = StubScheduler.random()
        assertSeq (items.flatten.map (_ * 2)) (batch (items) .map (_ * 2))
      }

      forAll (test)
    }

    "pass through an exception" in {
      implicit val scheduler = StubScheduler.random()
      assertFail [DistinguishedException] {
        for (x <- batch (items)) yield
          if (x == 3)
            throw new DistinguishedException
          else
            x * 2
      }}}

  "flatMap should" - {

    def inner = Iterable (1, 2, 3)

    "work with the for keyword" in {
      implicit val scheduler = StubScheduler.random()
      val iter = for (x <- batch (items); y <- inner) yield (x, y)
      val expected = for (x <- items.flatten; y <- inner) yield (x, y)
      assertSeq (expected) (iter)
    }

    "handle various batches" in {

      def test (items: Seq [Seq [Int]]) {
        implicit val scheduler = StubScheduler.random()
        val iter = batch (items) .flatMap (x => inner.map (y => (x, y)))
        val expected = for (x <- items.flatten; y <- inner) yield (x, y)
        assertSeq (expected) (iter)
      }

      forAll (test)
    }

    "pass through an exception" in {
      implicit val scheduler = StubScheduler.random()
      assertFail [DistinguishedException] {
        for (x <- batch (items); y <- inner) yield
          if (y == 2)
            throw new DistinguishedException
          else
            (x, y)
      }}}

  "filter should" - {

    "work with the for keyword" in {
      implicit val scheduler = StubScheduler.random()
      val iter = for (x <- batch (items); if isOdd (x)) yield x
      assertSeq (items.flatten.filter (isOdd _)) (iter)
    }

    "handle various batches" in {

      def test (items: Seq [Seq [Int]]) {
        implicit val scheduler = StubScheduler.random()
        assertSeq (items.flatten.filter (isOdd _)) (batch (items) .filter (isOdd _))
      }

      forAll (test)
    }

    "pass through an exception" in {
      implicit val scheduler = StubScheduler.random()
      assertFail [DistinguishedException] {
        for {
          x <- batch (items)
          if x == 3 && (throw new DistinguishedException)
        } yield x
      }}}

  "batchFlatMap should" - {

    "handle various batches" in {

      def test (items: Seq [Seq [Int]]) {
        implicit val scheduler = StubScheduler.random()
        val iter = batch (items) .batchFlatMap (x => batch (items) .map (y => (x, y)))
        val expected = for (x <- items.flatten; y <- items.flatten) yield (x, y)
        assertSeq (expected) (iter)
      }

      forAll (test)
    }}

  "whilst should" - {

    "handle an empty input iterator" in {
      implicit val scheduler = StubScheduler.random()
      var seen = Set.empty [Int]
      val last = batch [Int] (Seq.empty) .whilst (_ => true) (seen += _) .expectPass()
      assertResult (Set.empty) (seen)
      assertResult (None) (last)
    }

    "stop when the condition fails immediately" in {
      implicit val scheduler = StubScheduler.random()
      var seen = Set.empty [Int]
      val last = batch (items) .whilst (_ < 0) (seen += _) .expectPass()
      assertResult (Set.empty) (seen)
      assertResult (Some (1)) (last)
    }

    "stop when the condition fails before the iterator stops" in {
      implicit val scheduler = StubScheduler.random()
      var seen = Set.empty [Int]
      val last = batch (items) .whilst (_ < 3) (seen += _) .expectPass()
      assertResult (Set (1, 2)) (seen)
      assertResult (Some (3)) (last)
    }

    "stop when the input iterator stops before the condition fails" in {
      implicit val scheduler = StubScheduler.random()
      var seen = Set.empty [Int]
      val last = batch (items) .whilst (_ < 6) (seen += _) .expectPass()
      assertResult (Set (1, 2, 3, 4, 5)) (seen)
      assertResult (None) (last)
    }

    "pass througn an exception from the input iterator" in {
      implicit val scheduler = StubScheduler.random()
      var seen = Set.empty [Int]
      val iter =
        for (x <- batch (items)) yield
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
      batch (items) .whilst { x =>
        if (x == 3)
          throw new DistinguishedException
        x < 5
      } (seen += _) .fail [DistinguishedException]
      assertResult (Set (1, 2)) (seen)
    }

    "pass through an exception from the body" in {
      implicit val scheduler = StubScheduler.random()
      var seen = Set.empty [Int]
      batch (items) .whilst (_ < 5) { x =>
        if (x == 3) throw new DistinguishedException
        seen += x
      } .fail [DistinguishedException]
      assertResult (Set (1, 2)) (seen)
    }}

  // The implementation of toMap is very similar to toSeq.
  "toSeq should" - {

    "handle various batches" in {

      def test (items: Seq [Seq [Int]]) {
        implicit val scheduler = StubScheduler.random()
        assertResult (items.flatten) (batch (items) .toSeq.expectPass())
      }

      forAll (test)
    }}

  // The implementation of toMapWhile is very similar to toSeqWhile.
  "toSeqWhile should" - {

    "handle various batches" in {

      def count (n: Int): Int => Boolean =
        new Function [Int, Boolean] {
          var count = n
          def apply (i: Int): Boolean = {
            count -= 1
            count >= 0
          }}

      def test (n: Int) (items: Seq [Seq [Int]]) {
        implicit val scheduler = StubScheduler.random()
        val out = items.flatten.take (n)
        val last = items.flatten.drop (n) .headOption
        assertResult ((out, last)) {
          batch (items) .toSeqWhile (count (n)) .expectPass()
        }}

      forAll (test (0))
      forAll (test (3))
      forAll (test (5))
    }}}
