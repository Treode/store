package com.treode.async

import java.lang.{Integer => JavaInt, Long => JavaLong}
import java.nio.channels.CompletionHandler
import scala.language.experimental.macros
import scala.reflect.macros.Context

trait Callback [-T] extends (T => Unit) {

  protected def pass (v: T)

  def fail (t: Throwable)

  def apply (v: T): Unit = pass (v)
}

object Callback {

  /** Adapts Callback to Java's NIO CompletionHandler. */
  object IntHandler extends CompletionHandler [JavaInt, Callback [Int]] {
    def completed (v: JavaInt, cb: Callback [Int]) = cb (v)
    def failed (t: Throwable, cb: Callback [Int]) = cb.fail (t)
  }

  /** Adapts Callback to Java's NIO CompletionHandler. */
  object LongHandler extends CompletionHandler [JavaLong, Callback [Long]] {
    def completed (v: JavaLong, cb: Callback [Long]) = cb (v)
    def failed (t: Throwable, cb: Callback [Long]) = cb.fail (t)
  }

  /** Adapts Callback to Java's NIO CompletionHandler. */
  object UnitHandler extends CompletionHandler [Void, Callback [Unit]] {
    def completed (v: Void, cb: Callback [Unit]) = cb()
    def failed (t: Throwable, cb: Callback [Unit]) = cb.fail (t)
  }

  def ignore [A]: Callback [A] =
    new Callback [A] {
      def pass (v: A): Unit = ()
      def fail (t: Throwable): Unit = throw t
    }

  def latch [A] (count: Int, cb: Callback [Unit]): Callback [A] =
    new CountingLatch (count, cb)

  def array [A] (count: Int, cb: Callback [Array [A]]) (implicit m: Manifest [A]): Callback [(Int, A)] =
    new ArrayLatch (count, cb)

  def map [K, V] (count: Int, cb: Callback [Map [K, V]]): Callback [(K, V)] =
    new MapLatch (count, cb)

  def seq [A] (count: Int, cb: Callback [Seq [A]]) (implicit m: Manifest [A]): Callback [A] =
    new SeqLatch (count, cb)
}
