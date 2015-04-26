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
import scala.collection.JavaConversions._
import scala.collection.mutable.Builder
import scala.util.{Failure, Success}

import com.treode.async.implicits._

import Async.{async, guard, supply, when}

/** Iterate batches of items asynchronously.
  *
  * Compare to [[AsyncIterator]]. The `foreach` and `whilst` methods here take a plain old
  * function, whereas those methods in AsyncIterator take an asynchronous function.
  *
  * Concrete classes should implement `batch`.
  */
trait BatchIterator [A] {
  self =>

  /** Execute the asynchronous operation `f` foreach batch of elements. */
  def batch (f: Iterable [A] => Async [Unit]): Async [Unit]

  def foreach (f: A => Unit): Async [Unit] =
    batch (xs => supply (xs foreach f))

  def map [B] (f: A => B): BatchIterator [B] =
    new BatchIterator [B] {
      def batch (g: Iterable [B] => Async [Unit]): Async [Unit] =
        self.batch (xs => g (xs map f))
    }

  def flatMap [B] (f: A => Iterable [B]): BatchIterator [B] =
    new BatchIterator [B] {
      def batch (g: Iterable [B] => Async [Unit]): Async [Unit] =
        self.batch (xs => g (xs flatMap f))
    }

  def filter (p: A => Boolean): BatchIterator [A] =
    new BatchIterator [A] {
      def batch (g: Iterable [A] => Async [Unit]): Async [Unit] =
        self.batch { xs =>
          val ys = xs filter p
          when (!ys.isEmpty) (g (ys))
        }}

  def withFilter (p: A => Boolean): BatchIterator [A] =
    filter (p)

  def batchFlatMap [B] (f: A => BatchIterator [B]) (implicit scheduler: Scheduler): BatchIterator [B] =
    new BatchIterator [B] {
      def batch (g: Iterable [B] => Async [Unit]): Async [Unit] =
        self.batch { b =>
          val i = b.iterator
          scheduler.whilst (i.hasNext) {
            f (i.next) .batch (g)
          }}}

  /** Flatten to iterate individual elements rather than batches of elements.
    *
    * This returns an asynchronous iterator that provides batched implementations for:
    *
    *   - [[AsyncIterator#toMap]]
    *   - [[AsyncIterator#toMapWhile]]
    *   - [[AsyncIterator#toSeq]]
    *   - [[AsyncIterator#toSeqWhile]]
    */
  def flatten (implicit scheduler: Scheduler): AsyncIterator [A] =
    new AsyncIterator [A] (self)

  /** Execute the operation `f` foreach element while `p` is true.  Return the first element for
    * which `p` fails, or `None` if `p` never fails and the whole sequence is consumed.
    *
    * @param p The condition on which to continue iterating.
    * @param f The function to invoke for each element matching `p`.
    * @return `None` if `p` always returned `true` and the entire sequence was consumed, or `Some`
    *   with the first element on which `p` returned `false`.
    */
  def whilst [B >: A] (p: A => Boolean) (f: A => Unit): Async [Option [B]] =
    async { close =>
      self.batch { b =>
        async { next =>
          val i = b.iterator
          var last = Option.empty [A]
          while (i.hasNext && last.isEmpty) {
            val x = i.next
            if (p (x))
              f (x)
            else
              last = Some (x)
          }
          last match {
            case Some (_) => close.pass (last)
            case None => next.pass (())
          }}
      } .run {
        case Success (v) => close.pass (None)
        case Failure (t) => close.fail (t)
      }}

  /** Iterate the entire batch iterator and build a standard map. */
  def toMap [K, V] (implicit w: <:< [A, (K, V)]): Async [Map [K, V]] = {
    val builder = Map.newBuilder [K, V]
    self
      .batch (xs => supply (builder ++= xs.asInstanceOf [Iterator [(K, V)]]))
      .map (_ => builder.result)
  }

  /** Iterate the asynchronous iterator while `p` is true and build a standard map.
    *
    * @param p The condition on which to continue adding elements to the map.
    * @return The map and the next element if there is one.  The next element is the first
    *   element for which `p` returned false.
    */
  def toMapWhile [K, V] (p: A => Boolean) (implicit w: <:< [A, (K, V)]): Async [(Map [K, V], Option [A])] = {
    val b = Map.newBuilder [K, V]
    self.whilst (p) (b += _) .map (last => (b.result, last))
  }

  /** Iterate the entire asynchronous iterator and build a standard sequence. */
  def toSeq: Async [Seq [A]] = {
    val builder = Seq.newBuilder [A]
    self
      .batch (xs => supply (builder ++= xs))
      .map (_ => builder.result)
  }

  /** Iterate the asynchronous iterator while `p` is true and build a standard sequence.
    *
    * @param p The condition on which to continue adding elements to the sequence.
    * @return The sequence and the next element if there is one.  The next element is the first
    *   element for which `p` returned false.
    */
  def toSeqWhile (p: A => Boolean): Async [(Seq [A], Option [A])] = {
    val b = Seq.newBuilder [A]
    self.whilst (p) (b += _) .map (n => (b.result, n))
  }}

object BatchIterator {

  def empty [A] =
    new BatchIterator [A] {
      def batch (f: Iterable [A] => Async [Unit]): Async [Unit] =
        async (_.pass (()))
    }

  /** Transform a Scala Iterable into an BatchIterator. */
  def adapt [A] (iter: Iterable [A]): BatchIterator [A] =
    new BatchIterator [A] {
      def batch (f: Iterable [A] => Async [Unit]): Async [Unit] =
        guard (f (iter))
    }

  /** Transform a Java Iterable into an BatchIterator. */
  def adapt [A] (iter: JIterable [A]): BatchIterator [A] =
    new BatchIterator [A] {
      def batch (f: Iterable [A] => Async [Unit]): Async [Unit] =
        guard (f (iter))
  }

  /** Given asynchronous iterators of sorted items, merge them into a single asynchronous iterator
    * that maintains the sort.  Keep duplicate elements, and when two or more input iterators
    * duplicate an element, first list the element from the earlier iterator, that is by position
    * in `iters`.
    */
  def merge [A] (
      iters: Seq [BatchIterator [A]]
  ) (implicit
      ordering: Ordering [A],
      scheduler: Scheduler
  ): BatchIterator [A] =
    new MergeIterator (iters)

  def make [A] (maker: => Async [BatchIterator [A]]): BatchIterator [A] =
    new BatchIterator [A] {
      def batch (f: Iterable [A] => Async [Unit]): Async [Unit] =
        maker.flatMap (_.batch (f))
    }}
