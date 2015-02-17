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
import scala.concurrent.{Future => ScalaFuture, ExecutionContext}
import scala.util.{Failure, Random, Success, Try}

package object implicits {

  implicit class RichCallback [A] (cb: Try [A] => Any) {

    /** Invoke this callback with [[scala.util.Success Success]]. */
    def pass (v: A): Unit = cb (Success (v))

    /** Invoke this callback with [[scala.util.Failure Failure]]. */
    def fail (t: Throwable) = cb (Failure (t))

    /** Invoke `f` immediately, and then invoke this callback if `f` throws an exception. */
    def defer (f: => Any) {
      try {
        f
      } catch {
        case t: Throwable => cb (Failure (t))
      }}

    /** Invoke `f` immediately, and then invoke this callback if `f` throws an exception or
      * returns [[scala.Some Some]].
      */
    def callback (f: => Option [A]) {
      Try (f) match {
        case Success (Some (v)) => cb (Success (v))
        case Success (None) => ()
        case Failure (t) => cb (Failure (t))
      }}

    /** Create a callback that will invoke `f` on [[scala.util.Success Success]], and then
      * invoke this callback if `f` throws an exception or returns [[scala.Some Some]].
      */
    def continue [B] (f: B => Option [A]): Callback [B] = {
      case Success (b) => callback (f (b))
      case Failure (t) => cb (Failure (t))
    }

    /** Experimental. */
    def timeout (fiber: Fiber, backoff: Backoff) (rouse: => Any) (implicit random: Random):
        TimeoutCallback [A] =
      new TimeoutCallback (fiber, backoff, rouse, cb)

    /** Create a callback that will run this one on the scheduler `s`. */
    def on (s: Scheduler): Callback [A] =
      (v => s.execute (cb, v))

    /** Create a callback that first invokes `f`, regardless of receiving
      * [[scala.util.Success Success]] or [[scala.util.Failure Failure]], and then invokes this
      * callback.
      */
    def ensure (f: => Any): Callback [A] = {
      case Success (v) =>
        cb (Try (f) .map (_ => v))
      case v @ Failure (t1) =>
        try {
          f
        } catch {
          case t2: Throwable => t1.addSuppressed (t2)
        }
        cb (v)
    }

    /** Create a callback that will invoke `f` on [[scala.util.Failure Failure]] and then invoke
      * this callback.
      */
    def recover (f: PartialFunction [Throwable, A]): Callback [A] =
      (v => cb (v recover f))

    /** Create a callback that will invoke `f` on [[scala.util.Failure Failure]] and then invoke
      * this callback.
      */
    def rescue (f: PartialFunction [Throwable, Try [A]]): Callback [A] =
      (v => cb (v recoverWith f))
  }

  implicit class RichIterable [A] (iter: Iterable [A]) {

    def async (implicit scheduler: Scheduler): AsyncIterator [A] =
      AsyncIterator.adapt (iter)

    def batch (implicit scheduler: Scheduler): BatchIterator [A] =
      BatchIterator.adapt (iter)

    def indexed = iter.zipWithIndex.latch

    object latch extends IterableLatch (iter)
  }

  implicit class RichJavaIterable [A] (iter: JIterable [A]) {

    def async (implicit scheduler: Scheduler): AsyncIterator [A] =
      AsyncIterator.adapt (iter)

    def batch (implicit scheduler: Scheduler): BatchIterator [A] =
      BatchIterator.adapt (iter)

    def indexed = iter.zipWithIndex.latch

    object latch extends IterableLatch (iter)
  }

  implicit class RichScalaFuture [A] (fut: ScalaFuture [A]) {

    def toAsync (implicit ctx: ExecutionContext): Async [A] =
      Async.async (fut.onComplete _)
  }}
