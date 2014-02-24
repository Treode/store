package com.treode.async

import java.lang.{Integer => JavaInt, Long => JavaLong}
import java.nio.channels.CompletionHandler
import scala.language.experimental.macros
import scala.reflect.macros.Context

trait Callback [-A] {

  def pass (v: A)

  def fail (t: Throwable)

  def callback [B] (f: B => A): Callback [B] = {
    val self = this
    new Callback [B] {
      def pass (b: B) {
        val a = try {
          f (b)
        } catch {
          case t: Throwable =>
            self.fail (t)
            return
        }
        self.pass (a)
      }
      def fail (t: Throwable): Unit = self.fail (t)
    }}

  def continue [B] (f: B => Any): Callback [B] = {
    val self = this
    new Callback [B] {
      def pass (b: B) {
        try {
          f (b)
        } catch {
          case t: Throwable => self.fail (t)
        }}
      def fail (t: Throwable): Unit = self.fail (t)
    }}

  def defer (f: => Any) {
    try {
      f
    } catch {
      case t: Throwable => fail (t)
    }}

  def invoke (f: => A) {
    val v = try {
      f
    } catch {
      case t: Throwable =>
        fail (t)
        return
    }
    pass (v)
  }

  def onError (f: => Any): Callback [A] = {
    val self = this
    new Callback [A] {
      def pass (v: A): Unit = self.pass (v)
      def fail (t: Throwable) {
        f
        self.fail (t)
      }}}

  def onLeave (f: => Any): Callback [A] = {
    val self = this
    new Callback [A] {
      def pass (v: A) {
        f
        self.pass (v)
      }
      def fail (t: Throwable) {
        f
        self.fail (t)
      }}}
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
