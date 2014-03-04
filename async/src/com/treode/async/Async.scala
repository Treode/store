package com.treode.async

import java.util.concurrent.{Future => JFuture, Callable, Executor, FutureTask, SynchronousQueue}

import Scheduler.toRunnable

trait Async [A] {

  def run (cb: Callback [A])

  def map [B] (f: A => B): Async [B] = {
    val self = this
    new Async [B] {
      def run (cb: Callback [B]) {
        self.run (new Callback [A] {
          def pass (a: A) {
            val b = try {
              f (a)
            } catch {
              case t: Throwable =>
                cb.fail (t)
                return
            }
            cb.pass (b)
          }
          def fail (t: Throwable): Unit = cb.fail (t)
        })
      }}}

  def flatMap [B] (f: A => Async [B]): Async [B] = {
    val self = this
    new Async [B] {
      def run (cb: Callback [B]) {
        self.run (new Callback [A] {
          def pass (v: A) {
            val a = try {
              f (v)
            } catch {
              case t: Throwable =>
                cb.fail (t)
                return
            }
            a run (cb)
          }
          def fail (t: Throwable): Unit = cb.fail (t)
        })
      }}}

  def filter (p: A => Boolean): Async [A] = {
    val self = this
    new Async [A] {
      def run (cb: Callback [A]) {
        self.run (new Callback [A] {
          def pass (v: A) {
            val c = try {
              p (v)
            } catch {
              case t: Throwable =>
                cb.fail (t)
                return
            }
            if (c) cb.pass (v)
          }
          def fail (t: Throwable): Unit = cb.fail (t)
        })
      }}}

  def withFilter (p: A => Boolean): Async [A] =
    filter (p)

  def flatten [B] (implicit witness: A <:< Async [B]): Async [B] =
    flatMap (task => task)

  def defer (cb: Callback [_]) {
    run (new Callback [A] {
      def pass (v: A): Unit = ()
      def fail (t: Throwable) = cb.fail (t)
    })
  }

  def onError (f: => Any): Async [A] = {
    val self = this
    new Async [A] {
      def run (cb: Callback [A]) {
        self.run (cb onError f)
      }}}

  def onLeave (f: => Any): Async [A] = {
    val self = this
    new Async [A] {
      def run (cb: Callback [A]) {
        self.run (cb onLeave f)
      }}}

  def await(): A = {
    val q = new SynchronousQueue [Either [Throwable, A]]
    run (new Callback [A] {
      def pass (v: A): Unit = q.offer (Right (v))
      def fail (t: Throwable): Unit = q.offer (Left (t))
    })
    q.take() match {
      case Right (v) => v
      case Left (e) => throw e
    }}

  def toFuture (implicit scheduler: Scheduler): Future [A] = {
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
        try {
          require (!ran, "Async was already run.")
          ran = true
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
    new Async [A] {
      def run (cb: Callback [A]): Unit = cb.pass (v)
    }

  def run [A] (cb: Callback [A]) (f: => Async [A]): Unit =
    guard (f) run (cb)

  def cond (p: => Boolean) (f: => Async [Unit]): Async [Unit] =
    new Async [Unit] {
      private var ran = false
      def run (cb: Callback [Unit]): Unit =
        try {
          require (!ran, "Async was already run.")
          ran = true
          if (p) f run cb else cb.pass()
        } catch {
          case e: Throwable => cb.fail (e)
        }}

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
