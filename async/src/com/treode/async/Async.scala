package com.treode.async

import java.util.concurrent.{Future => JFuture, Callable, Executor, FutureTask, SynchronousQueue}
import scala.runtime.NonLocalReturnControl
import scala.util.{Failure, Success, Try}

import Async.{_async, async}
import AsyncConversions._
import Scheduler.toRunnable

trait Async [A] {

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
            case Success (false) => ()
            case Failure (t) => cb (Failure (t))
          }
        case Failure (t) => cb (Failure (t))
      }}

  def withFilter (p: A => Boolean): Async [A] =
    filter (p)

  def flatten [B] (implicit witness: A <:< Async [B]): Async [B] =
    flatMap (task => task)

  def defer (cb: Callback [_]) {
    run {
      case Success (v) => ()
      case Failure (t) => cb (Failure (t))
    }}

  def on (s: Scheduler): Async [A] =
    _async (cb => run (cb on s))

  def leave (f: => Any): Async [A] =
    _async (cb => run (cb leave f))

  def recover (f: PartialFunction [Throwable, A]): Async [A] =
    _async (cb => run (cb recover f))

  def rescue (f: PartialFunction [Throwable, Try [A]]): Async [A] =
    _async (cb => run (cb rescue f))

  def await(): A = {
    val q = new OnceQueue [Try [A]]
    run (q.set _)
    q.await() match {
      case Success (v) => v
      case Failure (t) => throw t
    }}

  def toFuture: Future [A] = {
    val f = new Future [A]
    run (f)
    f
  }

  def toJavaFuture: JFuture [A] =
    new FutureTask (new Callable [A] {
      def call(): A = await()
    })
}

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

  def async [A] (f: Callback [A] => Any): Async [A] =
    new Async [A] {
      def run (cb: Callback [A]) {
        val c = new ExceptionCaptor (cb)
        try {
          f (c)
        } catch {
          case t: NonLocalReturnControl [_] => cb.fail (new ReturnNotAllowedFromAsync)
          case t: Throwable => cb.fail (t)
        }
        c.get
      }}

  def guard [A] (f: => Async [A]): Async [A] =
    try {
      f
    } catch {
      case t: NonLocalReturnControl [_] => t.value.asInstanceOf [Async [A]]
      case t: Throwable => _fail (t)
    }

  def supply [A] (f: => A): Async [A] =
    try {
      _pass (f)
    } catch {
      case t: NonLocalReturnControl [_] => _fail (new ReturnNotAllowedFromAsync)
      case t: Throwable => _fail (t)
    }

  def when [A] (p: => Boolean) (f: => Async [A]): Async [Unit] =
    try {
      if (p) f map (_ => ()) else _pass()
    } catch {
      case t: NonLocalReturnControl [_] => _fail (new ReturnNotAllowedFromAsync)
      case t: Throwable => _fail (t)
    }

  def latch [A, B] (a: Async [A], b: Async [B]): Async [(A, B)] =
    async { cb =>
      val latch = new PairLatch (cb)
      a run latch.cbA
      b run latch.cbB
    }

  def latch [A, B, C] (a: Async [A], b: Async [B], c: Async [C]): Async [(A, B, C)] =
    async { cb =>
      val latch = new TripleLatch (cb)
      a run latch.cbA
      b run latch.cbB
      c run latch.cbC
    }}
