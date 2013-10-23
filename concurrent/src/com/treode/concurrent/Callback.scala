package com.treode.concurrent

import java.lang.{Integer => JavaInt}
import java.nio.channels.CompletionHandler
import scala.language.experimental.macros
import scala.reflect.macros.Context

trait Callback [-T] extends (T => Unit) {

  protected def pass (v: T)

  def fail (t: Throwable)

  def apply (v: T): Unit =
    try {
      pass (v)
    } catch {
      case t: Throwable => fail (t)
    }}

object Callback {

  /** Adapts Callback to Java's NIO CompletionHandler. */
  object IntHandler extends CompletionHandler [JavaInt, Callback [Int]] {
    def completed (v: JavaInt, cb: Callback [Int]) = cb (v)
    def failed (t: Throwable, cb: Callback [Int]) = cb.fail (t)
  }

  /** Adapts Callback to Java's NIO CompletionHandler. */
  object UnitHandler extends CompletionHandler [Void, Callback [Unit]] {
    def completed (v: Void, cb: Callback [Unit]) = cb()
    def failed (t: Throwable, cb: Callback [Unit]) = cb.fail (t)
  }

  def _guard [A: c.WeakTypeTag]
      (c: Context) (cb: c.Expr [Callback [A]]) (f: c.Expr [Any]): c.Expr [Unit] = {
    import c.universe._
    reify {
      try {
        f.splice
      } catch {
        case t: Throwable => cb.splice.fail (t)
      }}}

  def guard [A] (cb: Callback [A]) (f: Any): Unit = macro _guard [A]

  def unary [A] (f: A => Any): Callback [A] =
    new Callback [A] {
      def pass (v: A): Unit = f (v)
      def fail (t: Throwable): Unit = throw t
    }

  def unary [A] (cb: Callback [_]) (f: A => Any): Callback [A] =
    new Callback [A] {
      def pass (v: A): Unit = f (v)
      def fail (t: Throwable): Unit = cb.fail (t)
    }

  def ignore [A]: Callback [A] =
    new Callback [A] {
      def pass (v: A): Unit = ()
      def fail (t: Throwable): Unit = throw t
    }

  def unit (f: => Any): Callback [Unit] =
    new Callback [Unit] {
      def pass (v: Unit): Unit = f
      def fail (t: Throwable): Unit = throw t
    }

  def unit (cb: Callback [_]) (f: => Any): Callback [Unit] =
    new Callback [Unit] {
      def pass (v: Unit): Unit = f
      def fail (t: Throwable): Unit = cb.fail (t)
    }

  def noop: Callback [Unit] =
    new Callback [Unit] {
      def pass (v: Unit): Unit = ()
      def fail (t: Throwable): Unit = throw t
    }}
