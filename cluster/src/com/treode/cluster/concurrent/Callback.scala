package com.treode.cluster.concurrent

trait Callback [-T] extends (T => Unit) {

  def fail (t: Throwable)
}

object Callback {

  def unary [A] (f: A => Any): Callback [A] =
    new Callback [A] {
      def apply (v: A): Unit = f (v)
      def fail (t: Throwable): Unit = throw t
    }

  def unary [A] (cb: Callback [_]) (f: A => Any): Callback [A] =
    new Callback [A] {
      def apply (v: A): Unit = f (v)
      def fail (t: Throwable): Unit = cb.fail (t)
    }

  def ignore [A]: Callback [A] =
    new Callback [A] {
      def apply (v: A): Unit = ()
      def fail (t: Throwable): Unit = throw t
    }

  def unit (f: => Any): Callback [Unit] =
    new Callback [Unit] {
      def apply (v: Unit): Unit = f
      def fail (t: Throwable): Unit = throw t
    }

  def unit (cb: Callback [_]) (f: => Any): Callback [Unit] =
    new Callback [Unit] {
      def apply (v: Unit): Unit = f
      def fail (t: Throwable): Unit = cb.fail (t)
    }

  def noop: Callback [Unit] =
    new Callback [Unit] {
      def apply (v: Unit): Unit = ()
      def fail (t: Throwable): Unit = throw t
    }
}
