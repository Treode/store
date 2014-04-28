package com.treode

import java.lang.{Integer => JavaInt, Long => JavaLong}
import java.nio.channels.CompletionHandler
import scala.util.{Failure, Random, Success, Try}

import com.treode.async.implicits._

package async {

  class CallbackException (thrown: Throwable) extends Exception (thrown)

  class ReturnNotAllowedFromAsync extends Exception {
    override def getMessage = "The return keyword is not allowed in an async definition."
  }}

package object async {

  type Callback [A] = Try [A] => Any

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

    def fix [A] (f: Callback [A] => Try [A] => Any): Callback [A] =
      new Callback [A] {
        def apply (v: Try [A]) = f (this) (v)
      }

    def fanout [A] (cbs: Traversable [Callback [A]], scheduler: Scheduler): Callback [A] =
      (v => cbs foreach (scheduler.execute (_, v)))

    def ignore [A]: Callback [A] = {
        case Success (v) => ()
        case Failure (t) => throw t
      }}}
