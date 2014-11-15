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
import scala.collection.JavaConversions._
import scala.collection.mutable.Builder
import scala.util.{Failure, Success}

import com.treode.async.implicits._

import Async.{async, guard, supply, when}

trait BatchIterator [A] {
  self =>

  /** Execute the asynchronous operation `f` foreach batch of elements. */
  def batch (f: Iterator [A] => Async [Unit]): Async [Unit]

  def foreach (f: A => Unit): Async [Unit] =
    batch (xs => supply (xs foreach f))

  def map [B] (f: A => B): BatchIterator [B] =
    new BatchIterator [B] {
      def batch (g: Iterator [B] => Async [Unit]): Async [Unit] =
        self.batch (xs => g (xs map f))
    }

  def flatMap [B] (f: A => BatchIterator [B]) (implicit scheduler: Scheduler): BatchIterator [B] =
    new BatchIterator [B] {
      def batch (g: Iterator [B] => Async [Unit]): Async [Unit] =
        self.batch { xs =>
          scheduler.whilst (xs.hasNext) {
            f (xs.next) .batch (g)
          }}}

  def filter (p: A => Boolean): BatchIterator [A] =
    new BatchIterator [A] {
      def batch (g: Iterator [A] => Async [Unit]): Async [Unit] =
        self.batch { xs =>
          val ys = xs filter p
          when (!ys.isEmpty) (g (ys))
        }}

  def withFilter (p: A => Boolean): BatchIterator [A] =
    filter (p)

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
      self.batch { xs =>
        async { next =>
          var last = Option.empty [A]
          while (xs.hasNext && last.isEmpty) {
            val x = xs.next
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
  def toMap [K, V] (implicit witness: <:< [A, (K, V)]): Async [Map [K, V]] = {
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
  }

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
    new AsyncIterator [A] {

      def foreach (f: A => Async [Unit]): Async [Unit] =
        self.batch (_.async.foreach (f))

      override def toMap [K, V] (implicit witness: <:< [A, (K, V)]): Async [Map [K, V]] =
        self.toMap

      override def toMapWhile [K, V] (p: A => Boolean) (implicit witness: <:< [A, (K, V)]):
          Async [(Map [K, V], Option [A])] =
        self.toMapWhile (p)

      override def toSeq: Async [Seq [A]] =
        self.toSeq

      override def toSeqWhile (p: A => Boolean): Async [(Seq [A], Option [A])] =
        self.toSeqWhile (p)
    }}

object BatchIterator {

  def empty [A] =
    new BatchIterator [A] {
      def batch (f: Iterator [A] => Async [Unit]): Async [Unit] =
        async (_.pass (()))
    }

  /** Transform a Scala iterator into an BatchIterator. */
  def adapt [A] (iter: Iterator [A]): BatchIterator [A] =
    new BatchIterator [A] {
      def batch (f: Iterator [A] => Async [Unit]): Async [Unit] =
        guard (f (iter))
    }

  /** Transform a Java iterator into an BatchIterator. */
  def adapt [A] (iter: JIterator [A]): BatchIterator [A] =
    new BatchIterator [A] {
      def batch (f: Iterator [A] => Async [Unit]): Async [Unit] =
        guard (f (iter))
  }}
