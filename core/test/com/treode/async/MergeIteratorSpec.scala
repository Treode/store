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

import scala.collection.mutable.PriorityQueue
import scala.math.Ordering

import com.treode.async.stubs.{AsyncChecks, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.tags.Periodic
import org.scalatest.FreeSpec

import Async.supply
import AsyncIteratorTestTools._

class MergeIteratorSpec extends FreeSpec with AsyncChecks {

  // The merge iterator composes multiple input interators, those inputs being provided in a
  // particular order. When two input iterators yield an equal element, the merge iterator should
  // take both elements, listing the one from the first input interator then the one from the
  // second. We use the ones digit of integers to track which input iterator an element came from.
  // We give the merge iterator an ordering that ignores the ones digit. If the iterator works,
  // the output will be sorted by the normal integer ordering.

  // Order that ignores the ones digit, for example 11 == 12.
  val ordering = new Ordering [Int] {

    def compare (x: Int, y: Int): Int =
      Ordering.Int.compare (x / 10, y / 10)
  }

  // The batches. The merge iterator assumes the inputs are sorted, and these lists ensure that.
  // The ones digit will be filled in later.
  val firsts = Seq (Seq.empty, Seq (10), Seq (10, 20), Seq (10, 20, 30))
  val seconds = Seq (Seq.empty, Seq (60), Seq (60, 70), Seq (60, 70, 80))

  // Batches for iterators of one batch.
  val ones =
    for (first <- firsts)
      yield Seq (first)

  // Batches for iterators of two batches.
  val twos =
    for (first <- firsts; second <- seconds)
      yield Seq (first, second)

  // Batches for iterators of varying numbers of batches.
  val varying = ones ++ twos

  def number (n: Int, xss: Seq [Seq [Int]]): Seq [Seq [Int]] =
    xss.map (_.map (_ + n))

  // Simple merge, without all the asynchronous batch complexity. This reveals the benefit of
  // using the ones digit to track which batch, and using
  // - The outer sequences are all the batch iterators.
  // - The second level sequences are the batches of one batch iterator.
  // - The third level (inner most) sequences are the elements of a batch.
  def merge (xsss: Seq [Seq [Seq [Int]]]): Seq [Int] =
    xsss.flatten.flatten.sorted

  def merge (xs: Seq [BatchIterator [Int]]) (implicit scheduler: StubScheduler) =
    BatchIterator.merge (xs) (ordering, scheduler) .toSeq.expectPass()

  def testStringOf [A] (xsss: Seq [Seq [Seq [A]]]): String =
    xsss.map (_.map (_.mkString ("[", ", ", "]")) .mkString ("[", ", ", "]")) .mkString ("; ")

  "The MergeIterator should" - {

    def test (_xsss:  Seq [Seq [Int]] *) {
      val xsss = for ((xss, n) <- _xsss.zipWithIndex) yield number (n+1, xss)
      implicit val scheduler = StubScheduler.random()
      val batches = for (xss <- xsss) yield batch (xss)
      assertResult (merge (xsss)) (merge (batches))
    }

    "work with no inputs" in {
      test ()
    }

    "work with one input" in {
      for (xss <- varying)
        test (xss)
    }

    "work with two inputs" in {
      for (first <- varying; second <- varying)
        test (first, second)
    }

    "work with varying inputs" taggedAs (Periodic) in {
      forAllRandoms { implicit random =>
        val inputs =
          // upto 7 inputs
          for (_ <- 0 until random.nextInt (8)) yield {
            // merge requires inputs are sorted, n is grown
            var n = 0
            // upto 7 batches per input
            for (_ <- 0 until random.nextInt (8)) yield {
              // upto 7 elements per batch
              Seq.fill (random.nextInt (8)) {
                n += random.nextInt (6)
                n * 10
              }}}
        test (inputs: _*)
      }}}}
