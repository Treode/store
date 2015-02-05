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

package com.treode.disk

import java.util.{ArrayDeque}
import scala.collection.mutable.UnrolledBuffer
import scala.reflect.ClassTag
import scala.util.{Failure, Success}
import com.treode.async.{Async, Callback, Fiber, Scheduler }, Async.async

/** A queue of messages. Senders (producers) send one or many messages, and receivers (consumers)
  * take all queue messages if any, or wait for the next message(s) sent. The queue maintains a
  * batch number that's incremented upon each delivery.
  */
private class Dispatcher [M] (
  private var counter: Long
) (implicit
  scheduler: Scheduler,
  mtag: ClassTag [M]
) {

  private type R = Callback [(Long, UnrolledBuffer [M])]
  private val fiber = new Fiber
  private var messages = new UnrolledBuffer [M]

  // TODO: visible for testing, make it private
  val receivers = new ArrayDeque [R]

  private def singleton (m: M): UnrolledBuffer [M] =
    UnrolledBuffer (m)

  /** Remove all messages from the queue. */
  private def drain(): UnrolledBuffer [M] = {
    val t = messages
    messages = new UnrolledBuffer
    t
  }

  /** Deliver the messages to the receiver. */
  private def engage (receiver: R, messages: UnrolledBuffer [M]) {
    counter += 1
    scheduler.pass (receiver, (counter, messages))
  }

  /** True if no messages or receivers are queued.
    * @return True if no messages or receivers are queued.
    */
  def isEmpty = messages.isEmpty && receivers.isEmpty

  /** Send the message. Deliver it with an awaiting receiver, or queue it for the next receiver to
    * arrive.
    * @param message The message to send.
    */
  def send (message: M): Unit = fiber.execute {
    if (receivers.isEmpty)
      messages += message
    else
      engage (receivers.remove(), singleton (message))
  }

  /** Send the messages. Deliver them to an awaiting receiver, or queue them for the next receiver
    * to arrive.
    * @param messages The messages to send; the UnrolledBuffer will be cleared.
    */
  def send (messages: UnrolledBuffer [M]): Unit = fiber.execute {
    this.messages.concat (messages)
    if (!this.messages.isEmpty && !receivers.isEmpty)
      engage (receivers.remove(), drain())
  }

  /** Deprecated. */
  def receive (receiver: (Long, UnrolledBuffer [M]) => Any): Unit =
    receive() run {
      case Success ( (counter, messages) ) => receiver(counter, messages)
      case Failure (thrown) => throw thrown
    }

  /** Receive all queued messages. The receiver gets all queued messages delivered promptly if any
    * any are queued, or the receiver gets called later when some sender provides messages.
    * @returns All messages that had been queued or recently sent.
    */
  def receive () : Async [(Long, UnrolledBuffer [M])] ={
    fiber.async {cb =>
      if(messages.isEmpty)
      receivers.add (cb)
    else
      engage (cb, drain())
    }}}
