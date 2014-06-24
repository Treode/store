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

  def adapt [A] (xs: A*) (implicit scheduler: StubScheduler): AsyncIterator [A] =
    AsyncIterator.adapt (xs.iterator)

  def track [A] (iter: AsyncIterator [A]) (f: A => Any): AsyncIterator [A] =
    new AsyncIterator [A] {
      def foreach (g: A => Async [Unit]): Async [Unit] = {
        iter.foreach { x =>
          f (x)
          g (x)
        }}}

  def failWhen [A] (iter: AsyncIterator [A]) (p: A => Boolean): AsyncIterator [A] =
    new AsyncIterator [A] {
      def foreach (g: A => Async [Unit]): Async [Unit] = {
        iter.foreach { x =>
          if (p (x)) throw new DistinguishedException
          g (x)
        }}}

  def failNow [A]: AsyncIterator [A] =
    new AsyncIterator [A] {
      def foreach (g: A => Async [Unit]): Async [Unit] =
        supply (throw new DistinguishedException)
    }

  def assertSeq [A] (expected: A*) (actual: AsyncIterator [A]) (implicit s: StubScheduler): Unit =
    assertResult (expected) (actual.toSeq.pass)

  def assertFail [E] (iter: AsyncIterator [_]) (implicit s: StubScheduler, m: Manifest [E]): Unit =
    iter.foreach (_ => supply()) .fail [E]
}
