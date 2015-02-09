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
import org.scalatest.Assertions

import Assertions.assertResult
import Async.supply

object AsyncIteratorTestTools {

  class DistinguishedException extends Exception

  val items = Seq (Seq (1, 2, 3, 4, 5))

  /** Run a test for several different batches. */
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

  /** A batch iterator over each given batch. */
  def batch [A] (xs: Seq [Seq [A]]) (implicit s: StubScheduler): BatchIterator [A] =
    new BatchIterator [A] {
      val i = xs.iterator
      def batch (f: Iterable [A] => Async [Unit]): Async [Unit] =
        s.whilst (i.hasNext) (f (i.next))
    }

  /** An async iterator over each given value. */
  def flatten [A] (xs: Seq [Seq [A]]) (implicit s: StubScheduler): AsyncIterator [A] =
    batch (xs) .flatten

  // Scala disambiguates on first argument list only; this works around that.
  class AssertSeq [A] (expected: Seq [A]) {

    def apply (actual: AsyncIterator [A]) (implicit scheduler: StubScheduler): Unit =
      assertResult (expected) (actual.toSeq.expectPass())

    def apply (actual: BatchIterator [A]) (implicit scheduler: StubScheduler): Unit =
      assertResult (expected) (actual.toSeq.expectPass())
  }

  def assertSeq [A] (expected: Seq [A]): AssertSeq [A] = new AssertSeq [A] (expected)

  def assertFail [E] (iter: BatchIterator [_]) (implicit s: StubScheduler, m: Manifest [E]): Unit =
    iter.foreach (_ => supply (())) .fail [E]

  def isOdd (x: Int): Boolean = (x & 1) == 0
}
