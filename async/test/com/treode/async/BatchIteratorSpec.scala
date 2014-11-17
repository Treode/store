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

import Async.supply
import AsyncIteratorTestTools._

class BatchIteratorSpec extends FreeSpec {

  val items = Seq (Seq (1, 2, 3, 4, 5))

  def testStringOf [A] (items: Seq [Seq [A]]): String =
    items.map (_.mkString ("[", ", ", "]")) .mkString ("[", ", ", "]")

  def forAll (test: Seq [Seq [Int]] => Any) {

    // No batches.
    test (Seq.empty)

    // One batch.
    test (Seq (Seq.empty))
    test (Seq (Seq (1)))
    test (Seq (Seq (1, 2)))
    test (Seq (Seq (1, 2, 3, 4, 5)))

    // Two batches.
    for {
      first <- Seq (Seq.empty, Seq (1), Seq (1, 2), Seq (1, 2, 3, 4, 5))
      second <- Seq (Seq.empty, Seq (6), Seq (6, 7), Seq (6, 7, 8, 9, 10))
    } {
      test (Seq (first, second))
    }

    // Three batches.
    for {
      first <- Seq (Seq.empty, Seq (1), Seq (1, 2), Seq (1, 2, 3, 4, 5))
      second <- Seq (Seq.empty, Seq (6), Seq (6, 7), Seq (6, 7, 8, 9, 10))
      third <- Seq (Seq.empty, Seq (11), Seq (11, 12), Seq (11, 12, 13, 14, 15))
    } {
      test (Seq (first, second, third))
    }}

  "BatchIterator should" - {

    // The implementation of toMap is very similar to toSeq.
    "handle toSeq" in {

      def test (items: Seq [Seq [Int]]) {
        implicit val scheduler = StubScheduler.random()
        assertResult (items.flatten) (batch (items) .toSeq.expectPass())
      }

      forAll (test)
    }

    // The implementation of toMapWhile is very similar to toSeqWhile; also tests whilst.
    "handle toSeqWhile" in {

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
    }}

  "BatchIterator.foreach should" - {

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
    }}

  "BatchIterator.map should" - {

    "work with the for keyword" in {
      implicit val scheduler = StubScheduler.random()
      val iter = for (x <- batch (items)) yield x * 2
      assertSeq (items.flatten.map (_ * 2): _*) (iter)
    }

    "handle various batches" in {

      def test (items: Seq [Seq [Int]]) {
        implicit val scheduler = StubScheduler.random()
        assertSeq (items.flatten.map (_ * 2): _*) (batch (items) .map (_ * 2))
      }

      forAll (test)
    }}

  "BatchIterator.flatMap should" - {

    "work with the for keyword" in {
      implicit val scheduler = StubScheduler.random()
      val iter = for (x <- batch (items); y <- Iterator (3, 4)) yield (x, y)
      val expected = items.flatten.flatMap (x => Iterator (3, 4) .map (y => (x, y)))
      assertSeq (expected: _*) (iter)
    }

    "handle various batches" in {

      def test (items: Seq [Seq [Int]]) {
        implicit val scheduler = StubScheduler.random()
        val iter = batch (items) .flatMap (x => Iterator (1, 2, 3) .map (y => (x, y)))
        val expected = for (x <- items.flatten; y <- Iterator (1, 2, 3)) yield (x, y)
        assertSeq (expected: _*) (iter)
      }

      forAll (test)
    }}

  "BatchIterator.filter should" - {

    def isOdd (x: Int): Boolean = (x & 1) == 0

    "work with the for keyword" in {
      implicit val scheduler = StubScheduler.random()
      val iter = for (x <- batch (items); if isOdd (x)) yield x
      assertSeq (items.flatten.filter (isOdd _): _*) (iter)
    }

    "handle various batches" in {

      def test (items: Seq [Seq [Int]]) {
        implicit val scheduler = StubScheduler.random()
        assertSeq (items.flatten.filter (isOdd _): _*) (batch (items) .filter (isOdd _))
      }

      forAll (test)
    }}

  "BatchIterator.batchFlatMap should" - {

    "handle various batches" in {

      def test (items: Seq [Seq [Int]]) {
        implicit val scheduler = StubScheduler.random()
        val iter = batch (items) .batchFlatMap (x => batch (items) .map (y => (x, y)))
        val expected = for (x <- items.flatten; y <- items.flatten) yield (x, y)
        assertSeq (expected: _*) (iter)
      }

      forAll (test)
    }}

  "BatchIterator.flatten should" - {

    "handle foreach" in {

      def test (items: Seq [Seq [Int]]) {
        implicit val scheduler = StubScheduler.random()
        val builder = Seq.newBuilder [Int]
        batch (items) .flatten.foreach (x => supply (builder += x)) .expectPass()
        assertResult (items.flatten) (builder.result)
      }

      forAll (test)
    }}}
