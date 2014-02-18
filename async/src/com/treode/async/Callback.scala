package com.treode.async

import java.lang.{Integer => JavaInt, Long => JavaLong}
import java.nio.channels.CompletionHandler
import scala.language.experimental.macros
import scala.reflect.macros.Context

trait Callback [-T] {

  def pass (v: T)
  def fail (t: Throwable)
}

object Callback {

  /** Adapts Callback to Java's NIO CompletionHandler. */
  object IntHandler extends CompletionHandler [JavaInt, Callback [Int]] {
    def completed (v: JavaInt, cb: Callback [Int]) = cb.pass (v)
    def failed (t: Throwable, cb: Callback [Int]) = cb.fail (t)
  }

  /** Adapts Callback to Java's NIO CompletionHandler. */
  object LongHandler extends CompletionHandler [JavaLong, Callback [Long]] {
    def completed (v: JavaLong, cb: Callback [Long]) = cb.pass (v)
    def failed (t: Throwable, cb: Callback [Long]) = cb.fail (t)
  }

  /** Adapts Callback to Java's NIO CompletionHandler. */
  object UnitHandler extends CompletionHandler [Void, Callback [Unit]] {
    def completed (v: Void, cb: Callback [Unit]) = cb.pass()
    def failed (t: Throwable, cb: Callback [Unit]) = cb.fail (t)
  }

  def continue [A] (cb: Callback [_]) (f: A => Any): Callback [A] =
    new Callback [A] {
      def pass (v: A) {
        try {
          f (v)
        } catch {
          case t: Throwable => cb.fail (t)
        }}
      def fail (t: Throwable): Unit = cb.fail (t)
    }

  def defer [A] (cb: Callback [A]) (f: => Any): Unit =
    try {
      f
    } catch {
      case t: Throwable => cb.fail (t)
    }

  def fanout [A] (cbs: Traversable [Callback [A]], scheduler: Scheduler): Callback [A] =
    new Callback [A] {
      def pass (v: A): Unit = cbs foreach (scheduler.pass (_, v))
      def fail (t: Throwable): Unit = cbs foreach (scheduler.fail (_, t))
    }

  def ignore [A]: Callback [A] =
    new Callback [A] {
      def pass (v: A): Unit = ()
      def fail (t: Throwable): Unit = throw t
    }}
