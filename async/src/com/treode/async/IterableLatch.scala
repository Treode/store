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

import scala.collection.generic.FilterMonadic

import Async.async

/** Run asynchronous operations simultaneously across a collection, await the completion of all
  * those operations, perhaps collecting the results.
  *
  * {{{
  * import com.treode.async.implicits._
  *
  * // Await completions, ignore results.
  * def process (xs: Seq [Int]): Async [Unit] =
  *   for (x <- xs.latch)
  *     // Some asynchronous operation using x.
  *
  * // Await completions, ignore results.
  * def process (xs: Seq [Int]): Async [Unit] =
  *   xs.latch (x => ...)
  *
  * // Await completions, collect results in any order.
  * def process (xs: Seq [Int]): Async [Seq [B]] =
  *   for (x <- xs.latch.collect) yield
  *     // Some asynchronous operation using x, yielding a B.
  *
  * // Await completions, collect results in any order.
  * def process (xs: Seq [Int]): Async [Seq [B]] =
  *   xs.latch.collect (x => ...)
  *
  * // Await completions, collect results in a map.
  * def process (xs: Seq [Int]): Async [Map [K, V]] =
  *   for (x <- xs.latch.collate) yield
  *     // Some asynchronous operation using x, yielding (K, V).
  *
  * // Await completions, collect results in a map.
  * def process (xs: Seq [Int]): Async [Map [K, V]] =
  *    xs.latch.collate (x => ...)
  *
  * // Await completions, collect results in input order.
  * def process (xs: Seq [Int]): Async [Seq [B]] =
  *   for ((x, i) <- xs.indexed) yield
  *     // Some asynchronous operation using x and i, yielding a B.
  * }}}
  */
class IterableLatch [A, R] private [async] (iter: FilterMonadic [A, R], size: Int) {

  private def run [A, R, B] (iter: FilterMonadic [A, R], cb: Callback [B]) (f: A => Async [B]): Unit =
    iter foreach (x => f (x) run (cb))

  def apply [B] (f: A => Async [B]): Async [Unit] =
    async { cb =>
      run [A, R, B] (iter, new CountingLatch [B] (size, cb)) (f)
    }

  def foreach [B] (f: A => Async [B]): Async [Unit] =
    apply (f)

  def map [C, B] (f: A => Async [B]) (implicit m: Manifest [B], w: A <:< (C, Int)): Async [Seq [B]] =
    async { cb =>
      run [A, R, (Int, B)] (iter, new ArrayLatch (size, cb)) {
        x => f (x) map ((x._2, _))
      }}

  def filter (p: A => Boolean) =
    new IterableLatch (iter.withFilter (p), size)

  def withFilter (p: A => Boolean) =
    new IterableLatch (iter.withFilter (p), size)

  /** The iterated method must yield a result; the final result is a sequence whose order depends
    * on the scheduling of the asynchronous operations.
    */
  object collect {

    def apply [B] (f: A => Async [B]) (implicit m: Manifest [B]): Async [Seq [B]] =
      async { cb =>
        run [A, R, B] (iter, new CasualLatch [B] (size, cb)) (f)
      }

    def map [B] (f: A => Async [B]) (implicit m: Manifest [B]): Async [Seq [B]] =
      apply (f)

    def filter (p: A => Boolean) =
      new IterableLatch (iter.withFilter (p), size) .collect

    def withFilter (p: A => Boolean) =
      new IterableLatch (iter.withFilter (p), size) .collect
  }

  /** The iterated method must yield a pair (key, value); the final result is a map. */
  object collate {

    def apply [K, V] (f: A => Async [(K, V)]): Async [Map [K, V]] =
      async { cb =>
        run [A, R, (K, V)] (iter, new MapLatch [K, V] (size, cb)) (f)
      }

    def map [K, V] (f: A => Async [(K, V)]): Async [Map [K, V]] =
      apply (f)

    def filter (p: A => Boolean) =
      new IterableLatch (iter.withFilter (p), size) .collate

    def withFilter (p: A => Boolean) =
      new IterableLatch (iter.withFilter (p), size) .collate
  }}
