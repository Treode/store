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

import java.util.{Iterator => JIterator}
import scala.util.{Failure, Success}

import com.treode.async.implicits._

import Async.{async, supply, when}

/** Concrete classes should implement `foreach`. */
trait AsyncIterator [A] {
  self =>

  /** Execute the asynchronous operation `f` foreach element. */
  def foreach (f: A => Async [Unit]): Async [Unit]

  def map [B] (f: A => B): AsyncIterator [B] =
    new AsyncIterator [B] {
      def foreach (g: B => Async [Unit]): Async [Unit] =
        self.foreach (x => g (f (x)))
    }

  def flatMap [B] (f: A => AsyncIterator [B]): AsyncIterator [B] =
    new AsyncIterator [B] {
      def foreach (g: B => Async [Unit]): Async [Unit] =
        self.foreach (x => f (x) foreach (g))
    }

  def filter (p: A => Boolean): AsyncIterator [A] =
    new AsyncIterator [A] {
      def foreach (g: A => Async [Unit]): Async [Unit] =
        self.foreach (x => when (p (x)) (g (x)))
    }

  def withFilter (p: A => Boolean): AsyncIterator [A] =
    filter (p)

  /** A BatchIterator of sequences of one element. */
  def batch: BatchIterator [A] =
    new BatchIterator [A] {
      def batch (f: Iterator [A] => Async [Unit]): Async [Unit] =
        self.foreach (x => f (Iterator (x)))
    }

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
  def toMap [K, V] (implicit witness: <:< [A, (K, V)]): Async [Map [K, V]] = {
    val builder = Map.newBuilder [K, V]
    foreach (x => supply (builder += x)) .map (_ => builder.result)
  }

  /** Iterate the asynchronous iterator while `p` is true and build a standard map.
    *
    * @param p The condition on which to continue adding elements to the map.
    * @return The map and the next element if there is one.  The next element is the first
    *   element for which `p` returned false.
    */
  def toMapWhile [K, V] (p: A => Boolean) (implicit witness: <:< [A, (K, V)]):
      Async [(Map [K, V], Option [A])] = {
    val builder = Map.newBuilder [K, V]
    whilst (p) (x => supply (builder += x)) .map (n => (builder.result, n))
  }

  /** Iterate the entire asynchronous iterator and build a standard sequence. */
  def toSeq: Async [Seq [A]] = {
    val builder = Seq.newBuilder [A]
    foreach (x => supply (builder += x)) .map (_ => builder.result)
  }

  /** Iterate the asynchronous iterator while `p` is true and build a standard sequence.
    *
    * @param p The condition on which to continue adding elements to the sequence.
    * @return The sequence and the next element if there is one.  The next element is the first
    *   element for which `p` returned false.
    */
  def toSeqWhile (p: A => Boolean): Async [(Seq [A], Option [A])] = {
    val builder = Seq.newBuilder [A]
    whilst (p) (x => supply (builder += x)) .map (n => (builder.result, n))
  }}

object AsyncIterator {

  def empty [A] =
    new AsyncIterator [A] {
      def foreach (f: A => Async [Unit]): Async [Unit] =
        async (_.pass (()))
    }

  /** Transform a Scala iterator into an AsyncIterator. */
  def adapt [A] (iter: Iterator [A]) (implicit scheduler: Scheduler): AsyncIterator [A] =
    new AsyncIterator [A] {
      def foreach (f: A => Async [Unit]): Async [Unit] =
        scheduler.whilst (iter.hasNext) (f (iter.next))
    }

  /** Transform a Java iterator into an AsyncIterator. */
  def adapt [A] (iter: JIterator [A]) (implicit scheduler: Scheduler): AsyncIterator [A] =
    new AsyncIterator [A] {
      def foreach (f: A => Async [Unit]): Async [Unit] =
        scheduler.whilst (iter.hasNext) (f (iter.next))
  }

  def make [A] (maker: => Async [AsyncIterator [A]]): AsyncIterator [A] =
    new AsyncIterator [A] {
      def foreach (f: A => Async [Unit]): Async [Unit] =
        maker.flatMap (_.foreach (f))
    }

  /** Given asynchronous iterators of sorted items, merge them into a single asynchronous iterator
    * that maintains the sort.  Keep duplicate elements, and when two or more input iterators
    * duplicate an element, first list the element from the earlier iterator, that is by position
    * in `iters`.
    */
  def merge [A] (
      iters: Seq [AsyncIterator [A]]
  ) (implicit
      ordering: Ordering [A],
      scheduler: Scheduler
  ): AsyncIterator [A] =
    new MergeIterator (iters)
}
