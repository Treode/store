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

  /** An async iterator over each given value. */
  def adapt [A] (xs: A*) (implicit scheduler: StubScheduler): AsyncIterator [A] =
    AsyncIterator.adapt (xs.iterator)

  /** A batch iterator over each given batch. */
  def batch [A] (xs: Seq [Seq [A]]) (implicit scheduler: StubScheduler): BatchIterator [A] =
    new BatchIterator [A] {
      val iter = xs.iterator
      def batch (f: Iterator [A] => Async [Unit]): Async [Unit] =
        scheduler.whilst (iter.hasNext) (f (iter.next.iterator))
    }

  /** Invoke f each time an element is consumed from the iterator. */
  def track [A] (iter: AsyncIterator [A]) (f: A => Any): AsyncIterator [A] =
    new AsyncIterator [A] {
      def foreach (g: A => Async [Unit]): Async [Unit] = {
        iter.foreach { x =>
          f (x)
          g (x)
        }}}

  /** Adapt the foreach body to thrown an exception on the element where p is true. */
  def failWhen [A] (iter: AsyncIterator [A]) (p: A => Boolean): AsyncIterator [A] =
    new AsyncIterator [A] {
      def foreach (g: A => Async [Unit]): Async [Unit] = {
        iter.foreach { x =>
          if (p (x)) throw new DistinguishedException
          g (x)
        }}}

  /** Yield an exeption from foreach before consuming a single element. */
  def failNow [A]: AsyncIterator [A] =
    new AsyncIterator [A] {
      def foreach (g: A => Async [Unit]): Async [Unit] =
        supply (throw new DistinguishedException)
    }

  // Scala disambiguates on first argument list only; this works around that.
  class AssertSeq [A] (expected: Seq [A]) {

    def apply (actual: AsyncIterator [A]) (implicit scheduler: StubScheduler): Unit =
      assertResult (expected) (actual.toSeq.expectPass())

    def apply (actual: BatchIterator [A]) (implicit scheduler: StubScheduler): Unit =
      assertResult (expected) (actual.toSeq.expectPass())
  }

  def assertSeq [A] (expected: A*): AssertSeq [A] = new AssertSeq [A] (expected)

  def assertFail [E] (iter: AsyncIterator [_]) (implicit s: StubScheduler, m: Manifest [E]): Unit =
    iter.foreach (_ => supply (())) .fail [E]
}
