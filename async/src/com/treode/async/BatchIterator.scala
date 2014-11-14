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

import Async.{async, guard, supply}

trait BatchIterator [A] extends AsyncIterator [Iterator [A]] {
  self =>

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

      private def buildWhile [B, C] (
          p: A => Boolean
      ) (
          builder: Builder [B, C]
      ) (implicit
          witness: <:< [A, B]
      ): Async [(C, Option [A])] =

        async { close =>
          self.foreach { xs =>
            async { next =>
              var last = Option.empty [A]
              while (xs.hasNext && last.isEmpty) {
                val x = xs.next
                if (p (x))
                  builder += x
                else
                  last = Some (x)
              }
              last match {
                case Some (_) => close.pass ((builder.result, last))
                case None => next.pass (())
              }}
          } run {
            case Success (v) => close.pass ((builder.result, None))
            case Failure (t) => close.fail (t)
          }}

      def foreach (f: A => Async [Unit]): Async [Unit] =
        self.foreach (_.async.foreach (f))

      override def toMap [K, V] (implicit witness: <:< [A, (K, V)]): Async [Map [K, V]] = {
        val builder = Map.newBuilder [K, V]
        self
          .foreach (xs => supply (builder ++= xs.asInstanceOf [Iterator [(K, V)]]))
          .map (_ => builder.result)
      }

      override def toMapWhile [K, V] (p: A => Boolean) (implicit witness: <:< [A, (K, V)]):
          Async [(Map [K, V], Option [A])] =
        buildWhile (p) (Map.newBuilder [K, V])

      override def toSeq: Async [Seq [A]] = {
        val builder = Seq.newBuilder [A]
        self
          .foreach (xs => supply (builder ++= xs))
          .map (_ => builder.result)
      }

      override def toSeqWhile (p: A => Boolean): Async [(Seq [A], Option [A])] =
        buildWhile (p) (Seq.newBuilder [A])
    }}

object BatchIterator {

  def empty [A] =
    new BatchIterator [A] {
      def foreach (f: Iterator [A] => Async [Unit]): Async [Unit] =
        async (_.pass (()))
    }

  /** Transform a Scala iterator into an BatchIterator. */
  def adapt [A] (iter: Iterator [A]): BatchIterator [A] =
    new BatchIterator [A] {
      def foreach (f: Iterator [A] => Async [Unit]): Async [Unit] =
        guard (f (iter))
    }

  /** Transform a Java iterator into an BatchIterator. */
  def adapt [A] (iter: JIterator [A]): BatchIterator [A] =
    new BatchIterator [A] {
      def foreach (f: Iterator [A] => Async [Unit]): Async [Unit] =
        guard (f (iter))
  }}
