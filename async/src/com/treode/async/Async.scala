package com.treode.async

import java.util.concurrent.{Future => JFuture, Callable, Executor, FutureTask, SynchronousQueue}
import scala.util.{Failure, Success, Try}

import Async.async
import AsyncConversions._
import Scheduler.toRunnable

trait Async [A] {

  def run (cb: Callback [A])

  def map [B] (f: A => B): Async [B] =
    async { cb =>
      run {
        case Success (a) => cb (Try (f (a)))
        case Failure (t) => cb (Failure (t))
      }}

  def flatMap [B] (f: A => Async [B]): Async [B] =
    async { cb =>
      run {
        case Success (a) =>
          Try (f (a)) match {
            case Success (b) => b run (cb)
            case Failure (t) => cb (Failure (t))
          }
        case Failure (t) => cb (Failure (t))
      }}

  def filter (p: A => Boolean): Async [A] =
    async { cb =>
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

  def on (s: Scheduler): Async [A] = {
    val self = this
    new Async [A] {
      def run (cb: Callback [A]): Unit = self.run (s.take (cb))
    }}

  def leave (f: => Any): Async [A] = {
    val self = this
    new Async [A] {
      def run (cb: Callback [A]): Unit = self.run (cb leave f)
    }}

  def await(): A = {
    val q = new SynchronousQueue [Try [A]]
    run (q.offer _)
    q.take() match {
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

  def async [A] (f: Callback [A] => Any): Async [A] =
    new Async [A] {
      private var ran = false
      def run (cb: Callback [A]) {
        require (!ran, "Async was already run.")
        ran = true
        try {
          f (cb)
        } catch {
          case t: Throwable => cb.fail (t)
        }}}

  def guard [A] (f: => Async [A]): Async [A] =
    new Async [A] {
      private var ran = false
      def run (cb: Callback [A]) {
        require (!ran, "Async was already run.")
        ran = true
        val v = try {
          f
        } catch {
          case t: Throwable =>
            cb.fail (t)
            return
        }
        v run cb
      }}

  def supply [A] (v: A): Async [A] =
    async (_.pass (v))

  def run [A] (cb: Callback [A]) (f: => Async [A]): Unit =
    guard (f) run (cb)

  def defer (cb: Callback [_]) (f: => Async [_]): Unit =
    guard (f) defer (cb)

  def cond [A] (p: => Boolean) (f: => Async [A]): Async [Unit] =
    guard {
      if (p) f map (_ => ()) else supply()
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
