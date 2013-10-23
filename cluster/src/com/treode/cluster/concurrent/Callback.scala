package com.treode.cluster.concurrent

import java.lang.{Integer => JavaInt}
import java.nio.channels.CompletionHandler

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
