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

import java.util.concurrent.{Future => JavaFuture, Callable, FutureTask}
import scala.concurrent.{Future => ScalaFuture, Promise}
import scala.runtime.NonLocalReturnControl
import scala.util.{Failure, Success, Try}

import com.google.common.util.concurrent.{ListenableFuture => GuavaFuture, SettableFuture}
import com.treode.async.implicits._

import Async.{_async, async}
import Scheduler.toRunnable

/** An Async is the return type of an asynchronous method.  It captures the operation to perform,
  * and needs a callback before performing it.  You may provide a callback directly to [[run]],
  * You may also use the monad methods, the `to*Future` methods or [[await]]; all of these supply
  * a callback to run under the covers.
  *
  * For example, we can fill a [[com.treode.buffer.PagedBuffer buffer]] from a
  * [[com.treode.async.io.File file]] and have a callback invoked when the operation completes:
  *
  * {{{
  * import java.nio.file.Paths
  * import java.util.concurrent.Executors
  * import scala.util.{Failure, Success}
  * import com.treode.async.io.File
  * import com.treode.buffer.PagedBuffer
  *
  * val executor = Executors.newScheduledThreadPool (8)
  * val file = File.open (Paths.get ("file"), executor)
  * val input = PagedBuffer (12)
  * file.fill (input, 0, 1024) run {
  *   case Success (_) => // do something with input
  *   case Failure (t) => // do something with exception
  * }
  * }}}
  *
  * The Async class implements all monadic methods except `foreach` so you may use the `for`
  * syntax with `yield` to perform asynchronous operations one after the other:
  *
  * {{{
  * import com.treode.async.Async
  * import com.treode.async.io.File
  * import com.treode.buffer.PagedBuffer
  *
  * def copy (src: File, srcPos: Long, dst: File, dstPos: Long, len: Int): Async [Unit] = {
  *   val buf = PagedBuffer (12)
  *   for {
  *     _ <- src.fill (buf, srcPos, len)       // Fill reads at least len bytes, maybe more.
  *     _ = buf.writePos = buf.readPos + len   // Chop extra bytes read, if any.
  *     _ <- dst.flush (buf, dstPos)
  *   } yield ()
  * }
  * }}}
  *
  * To use asynchronous methods in a single-threaded context, the Async class provides the
  * [[await]] method that blocks the calling thread until the operation completes.
  *
  * {{{
  * val input = PagedBuffer (12)
  * file.fill (input, 0, 1024) .await()
  * // Current thread waits for read to complete.
  * }}}
  *
  * '''Remember''', you must eventually pass a callback to the an Async result, otherwise nothing
  * happens.  For example, there will be no effect if we call the `copy` method above as:
  *
  * {{{
  * copy (file1, 0, file2, 0, 1024)
  * }}}
  *
  * That line by itself merely creates an asynchronous command that the garbage collector will
  * eventually remove without ever running.  You must hand a callback to the [[run]] method, or
  * use one of the methods that does so for you.  These include [[await]], the monad methods, the
  * various `to*Future` methods.
  *
  * @define GuavaListenableFuture http://docs.guava-libraries.googlecode.com/git-history/release/javadoc/com/google/common/util/concurrent/ListenableFuture.html
  * @define JavaFuture http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/Future.html
  * @define ScalaFuture http://www.scala-lang.org/api/current/index.html#scala.concurrent.Future
  */
trait Async [A] {

  /** Run the asynchronous operation and invoke the callback when it completes. */
  def run (cb: Callback [A])

  def map [B] (f: A => B): Async [B] =
    _async { cb =>
      run {
        case Success (a) => cb (Try (f (a)))
        case Failure (t) => cb (Failure (t))
      }}

  def flatMap [B] (f: A => Async [B]): Async [B] =
    _async { cb =>
      run {
        case Success (a) =>
          Try (f (a)) match {
            case Success (b) => b run (cb)
            case Failure (t) => cb (Failure (t))
          }
        case Failure (t) => cb (Failure (t))
      }}

  def filter (p: A => Boolean): Async [A] =
    _async { cb =>
      run {
        case Success (v) =>
          Try (p (v)) match {
            case Success (true) => cb (Success (v))
            case Success (false) => cb (Failure (new NoSuchElementException))
            case Failure (t) => cb (Failure (t))
          }
        case Failure (t) => cb (Failure (t))
      }}

  def withFilter (p: A => Boolean): Async [A] =
    filter (p)

  def flatten [B] (implicit witness: A <:< Async [B]): Async [B] =
    flatMap (task => task)

  /** Invoke the callback (not yet supplied) on the scheduler `s`. */
  def on (s: Scheduler): Async [A] =
    _async (cb => run (cb on s))

  /** Run the method `f` after the operation completes, regardless of it succeeding or failing. */
  def ensure (f: => Any): Async [A] =
    _async (cb => run (cb ensure f))

  /** If the asynchronous operation fails, run the function `f` to attempt to recover. */
  def recover (f: PartialFunction [Throwable, A]): Async [A] =
    _async (cb => run (cb recover f))

  /** If the asynchronous operation fails, run the function `f` to attempt to rescue it. */
  def rescue (f: PartialFunction [Throwable, Try [A]]): Async [A] =
    _async (cb => run (cb rescue f))

  /** Ignore the result of the asynchronous operation. */
  def unit: Async [Unit] =
    _async { cb =>
      run {
        case Success (_) => cb (Success (()))
        case Failure (t) => cb (Failure (t))
      }}

  /** Block the current thread until the (formerly asynchronous) operation completes. */
  def await(): A = {
    val q = new OnceQueue [Try [A]]
    run (q.set _)
    q.await() match {
      case Success (v) => v
      case Failure (t) => throw t
    }}

  /** Begin the asynchronous operation and return an asynchronous
    * [[com.treode.async.Future Future]] to capture its result.
    */
  def toFuture: Future [A] = {
    val f = new Future [A]
    run (f)
    f
  }

  /** Begin the asynchronous operation and return a Guava
    * [[$GuavaListenableFuture ListenableFuture]]
    * to capture its result.
    */
  def toGuavaFuture: GuavaFuture [A] = {
    val f = SettableFuture.create [A]
    run {
      case Success (v) => f.set (v)
      case Failure (t) => f.setException (t)
    }
    f
  }

  /** Begin the asynchronous operation and return a Java
    * [[$JavaFuture Future]]
    * to capture its result.
    */
  def toJavaFuture: JavaFuture [A] = {
    val f = new FutureTask (new Callable [A] {
      def call(): A = await()
    })
    f.run()
    f
  }

  /** Begin the asynchronous operation and return a Scala
    * [[$ScalaFuture Future]] to capture its result.
    */
  def toScalaFuture: ScalaFuture [A] = {
    val p = Promise [A] ()
    run (p.complete _)
    p.future
  }}

object Async {

  private def _async [A] (f: Callback [A] => Any): Async [A] =
    new Async [A] {
      def run (cb: Callback [A]): Unit =
        f (cb)
    }

  private def _pass [A] (v: A): Async [A] =
    new Async [A] {
      def run (cb: Callback [A]): Unit =
        cb.pass (v)
    }

  private def _fail [A] (t: Throwable): Async [A] =
    new Async [A] {
      def run (cb: Callback [A]): Unit =
        cb.fail (t)
    }

  /** Capture the callback to create an asynchronous result. */
  def async [A] (f: Callback [A] => Any): Async [A] =
    new Async [A] {
      def run (cb: Callback [A]) {
        try {
          f { v =>
            try {
              cb (v)
            } catch {
              case t: CallbackException => throw t
              case t: Throwable => throw new CallbackException (t)
            }}
        } catch {
          case t: NonLocalReturnControl [_] => cb.fail (new ReturnException)
          case t: CallbackException => throw t.getCause
          case t: Throwable => cb.fail (t)
        }}}

  /** Guard an asynchronous operation so that all exceptions are passed to the callback and none
    * are thrown.
    */
  def guard [A] (f: => Async [A]): Async [A] =
    try {
      f
    } catch {
      case t: NonLocalReturnControl [_] => _fail (new ReturnException)
      case t: Throwable => _fail (t)
    }

  /** Transform an synchronous block into an asynchronous one. */
  def supply [A] (f: => A): Async [A] =
    try {
      _pass (f)
    } catch {
      case t: NonLocalReturnControl [_] => _fail (new ReturnException)
      case t: Throwable => _fail (t)
    }

  def at (millis: Int) (implicit s: Scheduler): Async [Unit] =
    _async (cb => s.at (millis) (cb (Success (()))))

  def delay (millis: Int) (implicit s: Scheduler): Async [Unit] =
    _async (cb => s.delay (millis) (cb (Success (()))))

  /** Perform the asynchronous operation `f` only when the predicate `p` is true. */
  def when [U] (p: => Boolean) (f: => Async [U]): Async [Unit] =
    try {
      if (p) f.unit else _pass (())
    } catch {
      case t: NonLocalReturnControl [_] => _fail (new ReturnException)
      case t: Throwable => _fail (t)
    }

  /** Perform the asynchronous operation `f` only when `v` is defined. */
  def when [A, U] (v: Option [A]) (f: A => Async [U]): Async [Unit] =
    try {
      v match {
        case Some (v) => f (v) .unit
        case None => _pass (())
      }
    } catch {
      case t: NonLocalReturnControl [_] => _fail (new ReturnException)
      case t: Throwable => _fail (t)
    }

  /** Run `f` simultaneously `n` times. Simultaneously launch `n` instances of the asynchrnous
    * operation `f`, and complete this asynchronous operation after the `n` instances of `f` have
    * all completed. Ignore the results from each instance.
    */
  def count [A] (n: Int) (f: => Async [A]): Async [Unit] =
    async { cb =>
      val latch = new CountingLatch [A] (cb)
      latch.start (n)
      for (_ <- 0 until n)
        f run (latch)
    }

  /** Run `f` simultaneously `n` times. Simultaneously launch `n` instances of the asynchrnous
    * operation `f`, and complete this asynchronous operation after the `n` instances of `f` have
    * all completed. Collect the result from each instance in the order that they finish.
    */
  def collect [A] (n: Int) (f: => Async [A]) (implicit m: Manifest [A]): Async [Seq [A]] =
    async { cb =>
      val latch = new CasualLatch (cb)
      latch.start (n)
      for (_ <- 0 until n)
        f run (latch)
    }

  /** Run `f` simultaneously `n` times. Simultaneously launch `n` instances of the asynchrnous
    * operation `f`, and complete this asynchronous operation after the `n` instances of `f` have
    * all completed. Collect the result from each instance in the order that they were launched.
    */
  def collate [A] (n: Int) (f: Int => Async [A]) (implicit m: Manifest [A]): Async [Seq [A]] =
    async { cb =>
      val latch = new ArrayLatch (cb)
      latch.start (n)
      for (i <- 0 until n)
        f (i) map (x => (i, x)) run (latch)
    }

  /** Await the completion of two simultaneous asynchronous operations. */
  def latch [A, B] (a: Async [A], b: Async [B]): Async [(A, B)] =
    async { cb =>
      val latch = new PairLatch (cb)
      a run latch.cbA
      b run latch.cbB
    }

  /** Await the completion of three simultaneous asynchronous operations. */
  def latch [A, B, C] (a: Async [A], b: Async [B], c: Async [C]): Async [(A, B, C)] =
    async { cb =>
      val latch = new TripleLatch (cb)
      a run latch.cbA
      b run latch.cbB
      c run latch.cbC
    }}
