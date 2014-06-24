/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode

import java.lang.{Integer => JavaInt, Long => JavaLong}
import java.nio.channels.CompletionHandler
import scala.util.{Failure, Random, Success, Try}

import com.treode.async.implicits._

package async {

  private class CallbackException (thrown: Throwable) extends Exception (thrown)

  private class ReturnException extends Exception {
    override def getMessage = "The return keyword is not allowed in an async definition."
  }}

/** The async package defines the [[com.treode.async.Async Async]] class which an asynchronous
  * method may return as its result, and it is a good place to begin reading.
  *
  * This package also defines asynchronous [[com.treode.async.io.File files]] and
  * [[com.treode.async.io.Socket sockets]], and it provides [[com.treode.async.Fiber fibers]] that
  * are not quite entirely unlike actors.
  *
  * See the [[com.treode.async.stubs stubs]] package for an overview of testing asynchronous
  * methods.
  */
package object async {

  type Callback [A] = Try [A] => Any

  object Callback {

    /** Adapts Callback to Java's NIO CompletionHandler. */
    private [async] object IntHandler extends CompletionHandler [JavaInt, Callback [Int]] {
      def completed (v: JavaInt, cb: Callback [Int]) = cb.pass (v)
      def failed (t: Throwable, cb: Callback [Int]) = cb.fail (t)
    }

    /** Adapts Callback to Java's NIO CompletionHandler. */
    private [async] object LongHandler extends CompletionHandler [JavaLong, Callback [Long]] {
      def completed (v: JavaLong, cb: Callback [Long]) = cb.pass (v)
      def failed (t: Throwable, cb: Callback [Long]) = cb.fail (t)
    }

    /** Adapts Callback to Java's NIO CompletionHandler. */
    private [async] object UnitHandler extends CompletionHandler [Void, Callback [Unit]] {
      def completed (v: Void, cb: Callback [Unit]) = cb.pass()
      def failed (t: Throwable, cb: Callback [Unit]) = cb.fail (t)
    }

    def fix [A] (f: Callback [A] => Try [A] => Any): Callback [A] =
      new Callback [A] {
        def apply (v: Try [A]) = f (this) (v)
      }

    def fanout [A] (cbs: Traversable [Callback [A]]) (implicit scheduler: Scheduler): Callback [A] =
      (v => cbs foreach (scheduler.execute (_, v)))

    def ignore [A]: Callback [A] = {
        case Success (v) => ()
        case Failure (t) => throw t
      }}}
