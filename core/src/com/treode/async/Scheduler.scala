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

package com.treode.async

import java.util.List
import java.util.concurrent._
import scala.runtime.NonLocalReturnControl
import scala.util.{Failure, Success, Try}

import com.treode.async.implicits._

import Async.async
import Scheduler.toRunnable

trait Scheduler extends Executor {
  self =>

  def execute (task: Runnable)

  def delay (millis: Long, task: Runnable)

  def at (millis: Long, task: Runnable)

  def execute (task: => Any): Unit =
    execute (toRunnable (task))

  def execute [A] (f: A => Any, v: A): Unit =
    execute (toRunnable (f, v))

  def execute [A] (task: Async [A], cb: Callback [A]): Unit =
    execute (toRunnable (task, cb))

  def delay (millis: Long) (task: => Any): Unit =
    delay (millis, toRunnable (task))

  def at (millis: Long) (task: => Any): Unit =
    at (millis, toRunnable (task))

  def sleep (millis: Long): Async [Unit] =
    async (cb => delay (millis) (pass (cb, ())))

  def awake (millis: Long): Async [Unit] =
    async (cb => at (millis) (pass (cb, ())))

  def pass [A] (cb: Callback [A], v: A): Unit =
    execute (cb, Success (v))

  def fail [A] (cb: Callback [A], t: Throwable): Unit =
    execute (cb, Failure (t))

  def whilst [U] (p: => Boolean) (f: => Async [U]): Async [Unit] =
    async { cb =>
      val loop = Callback.fix [U] { loop => {
        case Success (v) =>
          execute {
            try {
              if (p)
                f run loop
             else
                pass (cb, ())
            } catch {
              case t: Throwable => fail (cb, t)
            }
          }
        case Failure (t) => fail (cb, t)
      }}
      loop.pass (null.asInstanceOf [U])
    }

  /** Implements what is needed by AsynchronousFileChannel. */
  private [async] lazy val asExecutorService: ExecutorService =
    new AbstractExecutorService {

      def execute (task: Runnable): Unit =
        self.execute (task)

      def awaitTermination (timeout: Long, unit: TimeUnit): Boolean =
        throw new UnsupportedOperationException

      def isShutdown(): Boolean =
        throw new UnsupportedOperationException

      def isTerminated(): Boolean =
        throw new UnsupportedOperationException

      def shutdown(): Unit =
        throw new UnsupportedOperationException

      def shutdownNow(): List [Runnable] =
        throw new UnsupportedOperationException
  }}

object Scheduler {

  def apply (executor: ScheduledExecutorService): Scheduler =
    new ExecutorAdaptor (executor)

  def toRunnable (task: => Any): Runnable =
    new Runnable {
      def run() =
        try {
          task
        } catch {
          case t: NonLocalReturnControl [_] => ()
          case t: CallbackException => throw t.getCause
        }}

  def toRunnable [A] (f: A => Any, v: A): Runnable =
    new Runnable {
      def run() =
        try {
          f (v)
        } catch {
          case t: NonLocalReturnControl [_] => ()
          case t: CallbackException => throw t.getCause
        }}

  def toRunnable [A] (task: Async [A], cb: Callback [A]): Runnable =
    new Runnable {
      def run() =
        try {
          task run cb
        } catch {
          case t: NonLocalReturnControl [_] => ()
          case t: CallbackException => throw t.getCause
        }}}
