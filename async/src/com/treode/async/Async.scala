package com.treode.async

import java.util.concurrent.{Future => JFuture, Callable, FutureTask, SynchronousQueue}

trait Async [A] {

  def run (cb: Callback [A])

  def map [B] (f: A => B): Async [B] = {
    val self = this
    new Async [B] {
      def run (cb: Callback [B]) {
        self.run (new Callback [A] {
          def pass (v: A): Unit = cb.pass (f (v))
          def fail (t: Throwable): Unit = cb.fail (t)
        })
      }}}

  def flatMap [B] (f: A => Async [B] ): Async [B] = {
    val self = this
    new Async [B] {
      def run (cb: Callback [B]) {
        self.run (new Callback [A] {
          def pass (v: A): Unit = f (v) run (cb)
          def fail (t: Throwable): Unit = cb.fail (t)
        })
      }}}

  def filter (p: A => Boolean): Async [A] = {
    val self = this
    new Async [A] {
      def run (cb: Callback [A]) {
        self.run (new Callback [A] {
          def pass (v: A): Unit = if (p (v)) cb.pass (v)
          def fail (t: Throwable): Unit = cb.fail (t)
        })
      }}}

  def toFuture (implicit scheduler: Scheduler): Future [A] = {
    val f = new Future [A]
    run (f)
    f
  }

  def run(): A = {
    val q = new SynchronousQueue [Either [Throwable, A]]
    run (new Callback [A] {
      def pass (v: A): Unit = q.offer (Right (v))
      def fail (t: Throwable): Unit = q.offer (Left (t))
    })
    q.take() match {
      case Right (v) => v
      case Left (e) => throw e
    }}

  def toJavaFuture: JFuture [A] =
    new FutureTask (new Callable [A] {
      def call(): A = run()
    })
}

object Async {

  def async [A] (f: Callback [A] => Any): Async [A] =
    new Async [A] {
      def run (cb: Callback [A]) {
        try {
          f (cb)
        } catch {
          case e: Throwable => cb.fail (e)
        }}}

  def guard [A] (f: => Async [A]): Async [A] =
    new Async [A] {
      def run (cb: Callback [A]) {
        val v = try {
          f
        } catch {
          case e: Throwable =>
            cb.fail (e)
            return
        }
        v run cb
      }}

  def supply [A] (v: A): Async [A] =
    new Async [A] {
      def run (cb: Callback [A]): Unit = cb.pass (v)
    }

  def cond (p: => Boolean) (f: => Async [Unit]): Async [Unit] =
    new Async [Unit] {
      def run (cb: Callback [Unit]): Unit =
        try {
          if (p) f run cb else cb.pass()
        } catch {
          case e: Throwable => cb.fail (e)
        }}

  object whilst {

    def cb (p: => Boolean) (f: Callback [Unit] => Any) (implicit s: Scheduler): Async [Unit] =
      new Async [Unit] {
        def run (cb: Callback [Unit]) {
          val loop = new Callback [Unit] {
            def pass (v: Unit): Unit = s.execute {
              try {
                if (p)
                  f (this)
                else
                  s.pass (cb, ())
              } catch {
                case t: Throwable => s.fail (cb, t)
              }}
            def fail (t: Throwable): Unit = s.fail (cb, t)
          }
          loop.pass()
        }}

    def f (p: => Boolean) (f: => Any) (implicit s: Scheduler): Async [Unit] =
      cb (p) {cb => f; cb.pass()}

    def apply [A] (p: => Boolean) (f: => Async [Unit]) (implicit s: Scheduler): Async [Unit] =
      cb (p) {cb => f run cb}
  }}
