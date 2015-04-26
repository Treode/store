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

import java.lang.{Iterable => JIterable}
import scala.util.{Failure, Success}

import com.treode.async.implicits._

import Async.{async, supply, when}

/** Iterate items asynchronously.
  *
  * Compare to [[BatchIterator]]. The `foreach` and `whilst` methods here take an asynchronous
  * function, whereas those methods in BatchIterator take a plain old function.
  *
  * You should consider using BatchIterator whenever possible. AsyncIterator places a task on the
  * scheduler foreach element, and that can adds overhead. If you must do some asynchronous task
  * for every element, you will incur that overhead anyway, so the point is moot, and this
  * interface will be more convenient. On the other hand, if you can do that asynchronous task for
  * a batch of items, you can save that overhead by using BatchIterator.
  */
class AsyncIterator [A] (val batch: BatchIterator [A]) (implicit scheduler: Scheduler) {

  /** Execute the asynchronous operation `f` foreach element. */
  def foreach (f: A => Async [Unit]): Async [Unit] =
    batch.batch { b =>
      val xs = b.iterator
      scheduler.whilst (xs.hasNext) (f (xs.next))
    }

  def map [B] (f: A => B): AsyncIterator [B] =
    new AsyncIterator (batch.map (f))

  def flatMap [B] (f: A => AsyncIterator [B]): AsyncIterator [B] =
    new AsyncIterator (batch.batchFlatMap (x => f (x) .batch))

  def filter (p: A => Boolean): AsyncIterator [A] =
    new AsyncIterator (batch.filter (p))

  def withFilter (p: A => Boolean): AsyncIterator [A] =
    new AsyncIterator (batch.withFilter (p))

  /** Execute the asynchronous operation `f` foreach element while `p` is true.  Return the first
    * element for which `p` fails, or `None` if `p` never fails and the whole sequence is
    * consumed.
    *
    * @param p The condition on which to continue iterating.
    * @param f The function to invoke for each element matching `p`.
    * @return `None` if `p` always returned `true` and the entire sequence was consumed, or `Some`
    *   with the first element on which `p` returned `false`.
    */
  def whilst [B >: A] (p: A => Boolean) (f: A => Async [Unit]): Async [Option [B]] =
    async { close =>
      foreach { x =>
        async { next =>
          if (p (x))
            f (x) run (next)
          else
            close.pass (Some (x))
        }
      } run {
        case Success (v) => close.pass (None)
        case Failure (t) => close.fail (t)
      }}

  /** Iterate the entire asynchronous iterator and build a standard map. */
  def toMap [K, V] (implicit w: <:< [A, (K, V)]): Async [Map [K, V]] =
    batch.toMap

  /** Iterate the asynchronous iterator while `p` is true and build a standard map.
    *
    * @param p The condition on which to continue adding elements to the map.
    * @return The map and the next element if there is one.  The next element is the first
    *   element for which `p` returned false.
    */
  def toMapWhile [K, V] (p: A => Boolean) (implicit w: <:< [A, (K, V)]): Async [(Map [K, V], Option [A])] =
    batch.toMapWhile (p)

  /** Iterate the entire asynchronous iterator and build a standard sequence. */
  def toSeq: Async [Seq [A]] =
    batch.toSeq

  /** Iterate the asynchronous iterator while `p` is true and build a standard sequence.
    *
    * @param p The condition on which to continue adding elements to the sequence.
    * @return The sequence and the next element if there is one.  The next element is the first
    *   element for which `p` returned false.
    */
  def toSeqWhile (p: A => Boolean): Async [(Seq [A], Option [A])] =
    batch.toSeqWhile (p)
}

object AsyncIterator {

  def empty [A] (implicit scheduler: Scheduler): AsyncIterator [A] =
    new AsyncIterator (BatchIterator.empty)

  /** Transform a Scala Iterable into an AsyncIterator. */
  def adapt [A] (iter: Iterable [A]) (implicit scheduler: Scheduler): AsyncIterator [A] =
    new AsyncIterator (BatchIterator.adapt (iter))

  /** Transform a Java Iterable into an AsyncIterator. */
  def adapt [A] (iter: JIterable [A]) (implicit scheduler: Scheduler): AsyncIterator [A] =
    new AsyncIterator (BatchIterator.adapt (iter))

  def make [A] (maker: => Async [AsyncIterator [A]]) (implicit scheduler: Scheduler): AsyncIterator [A] =
    new AsyncIterator (BatchIterator.make (maker .map (_.batch)))
}
