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

import java.util.ArrayDeque
import scala.runtime.NonLocalReturnControl

/** Serialize operations; not quite entirely unlike Actors.
  *
  * A fiber provides a lightweight mechanism to serialize operations on an object.  As a simple
  * example, we will define a count down latch.
  *
  * {{{
  * import com.treode.async.{Callback, Fiber, Scheduler}
  * import com.treode.async.implicits._
  *
  * class CountDownLatch (private var count: Int) (implicit scheduler: Scheduler) {
  *
  *   private val fiber = new Fiber (scheduler)
  *   private var waiters = List.empty [Callback [Unit]]
  *
  *   // fiber.execute returns immediately to the caller; the method may be run latter.
  *   def decrement(): Unit = fiber.execute {
  *     // Exception is not thrown to caller of decrement; it is caught by the scheduler.
  *     assert (count > 0)
  *     count -= 1
  *     if (count == 0)
  *       Callback.fanout (waiters, scheduler) .pass()
  *     // Avoid memory leak.
  *     waiters = Nil
  *   }
  *
  *   // fiber.supply returns an Async immediately to the caller.  Method will be scheduled to run
  *   // when the caller provides a callback to the Async.  When the method completes, its result
  *   // will be passed to the callback.
  *   def get(): Async [Int] = fiber.supply {
  *     // Exception will be passed to callback.
  *     assert (count >= 0)
  *     count
  *   }
  *
  *   // fiber.async returns an Async immediately to the caller.  Method will be scheduled to run
  *   // when the caller provides a callback to the Async.  The method may invoke the callback or
  *   // store it somewhere to invoke latter.
  *   def await(): Async [Unit] = fiber.async { cb =>
  *     // Exception will be passed to callback.
  *     assert (count >= 0)
  *     if (count == 0)
  *       cb.pass()
  *     else
  *       waiters ::= cb
  *   }}
  * }}}
  *
  * In production, you will probably provide a multithreaded scheduler to your fibers.  To test
  * a system of interacting fibers, you can use [[com.treode.async.stubs.AsyncChecks AsyncChecks]]
  * and [[com.treode.async.stubs.StubScheduler StubScheduler]].
  *
  * == Comparison to Actors ==
  *
  * When you send a message to an actor
  * {{{
  * actor ! Message (value)
  * }}}
  * under the covers an object is created and placed on a queue.  The actor then processes that
  * queue one message at a time.
  *
  * When you invoke `fiber.execute`, under the covers a closure (also an object) is created and
  * placed on a queue.  The fiber then runs the closures one at a time.
  */
class Fiber (implicit scheduler: Scheduler) extends Scheduler {

  private [this] val tasks = new ArrayDeque [Runnable]
  private [this] var engaged = false

  private [this] def disengage(): Unit = synchronized {
    if (!tasks.isEmpty)
      scheduler.execute (tasks.remove)
    else
      engaged = false
  }

  private [this] def add (task: Runnable): Unit = synchronized {
    if (engaged) {
      tasks.add (task)
    } else {
      engaged = true
      scheduler.execute (task)
    }}

  def execute (task: Runnable): Unit =
    add (new Runnable {
      def run() {
        try {
          task.run()
        } finally {
          disengage()
        }}})

  def delay (millis: Long, task: Runnable): Unit =
    scheduler.delay (millis) (execute (task))

  def at (millis: Long, task: Runnable): Unit =
    scheduler.at (millis) (execute (task))

  def async [A] (task: Callback [A] => Any): Async [A] =
    Async.async { cb =>
      execute {
        try {
          task (cb)
        } catch {
          case t: NonLocalReturnControl [_] => scheduler.fail (cb, new ReturnException)
          case t: Throwable => scheduler.fail (cb, t)
        }}}

  def supply [A] (task: => A): Async [A] =
    Async.async { cb =>
      execute {
        try {
          scheduler.pass (cb, task)
        } catch {
          case t: NonLocalReturnControl [_] => scheduler.fail (cb, new ReturnException)
          case t: Throwable => scheduler.fail (cb, t)
        }}}}
