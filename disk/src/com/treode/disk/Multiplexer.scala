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

import scala.collection.mutable.UnrolledBuffer
import scala.reflect.ClassTag

import com.treode.async.{Async, Callback, Fiber, Scheduler}

import Callback.fanout

private class Multiplexer [M] (dispatcher: Dispatcher [M]) (
    implicit scheduler: Scheduler, mtag: ClassTag [M]) {

  private type R = (Long, UnrolledBuffer [M]) => Any

  private val fiber = new Fiber
  private var messages = new UnrolledBuffer [M]
  private var exclusive = false
  private var enrolled = false
  private var receiver = Option.empty [R]
  private var state: State = Open

  sealed abstract class State
  case object Open extends State
  case object Paused extends State
  case object Closed extends State
  case class Pausing (cbs: List [Callback [Unit]]) extends State
  case class Closing (cbs: List [Callback [Unit]]) extends State

  private def execute (f: State => Any): Unit =
    fiber.execute (f (state))

  private def async [A] (f: (Callback [A], State) => Any): Async [A] =
    fiber.async (cb => f (cb, state))

  private def singleton (m: M): UnrolledBuffer [M] =
    UnrolledBuffer (m)

  private def drain(): UnrolledBuffer [M] = {
    val t = messages
    messages = new UnrolledBuffer
    t
  }

  private def add (receiver: R): Unit =
    this.receiver = Some (receiver)

  private def remove(): R = {
    val t = receiver.get
    receiver = None
    t
  }

  private def deliver (receiver: R, exclusive: Boolean, batch: Long, messages: UnrolledBuffer [M]) {
    this.exclusive = exclusive
    receiver (batch, messages)
  }

  private def closedException =
    new IllegalStateException ("Multiplexer is closed.")

  def send (message: M): Unit = {
    val e = closedException
    execute {

    case Closing (_) =>
      throw e
    case Closed =>
      throw e
    case _ if receiver.isDefined =>
      deliver (remove(), true, 0L, singleton (message))
    case _ =>
      messages += message
  }}

  private def _dispatch (batch: Long, messages: UnrolledBuffer [M]): Unit = execute {
    case Open if receiver.isDefined =>
      assert (enrolled)
      enrolled = false
      deliver (remove(), false, batch, messages)
    case _ =>
      assert (enrolled)
      enrolled = false
      dispatcher.send (messages)
  }

  private val dispatch = (_dispatch _)

  def receive (receiver: R): Unit = execute {
    case _ if !messages.isEmpty =>
      deliver (receiver, true, 0L, drain())
    case Open if !enrolled =>
      enrolled = true
      add (receiver)
      dispatcher.receive (dispatch)
    case Open =>
      add (receiver)
    case Paused =>
      add (receiver)
    case Pausing (cbs) =>
      state = Paused
      add (receiver)
      scheduler.pass (fanout (cbs), ())
    case Closed =>
      ()
    case Closing (cbs) =>
      state = Closed
      scheduler.pass (fanout (cbs), ())
  }

  def send (rejects: UnrolledBuffer [M]): Unit = execute {
    case _ if exclusive =>
      messages = rejects.concat (messages)
    case _ =>
      assert (!enrolled)
      dispatcher.send (rejects)
  }

  def pause(): Async [Unit] = async {
    case (cb, Open) if receiver.isDefined =>
      state = Paused
      scheduler.pass (cb, ())
    case (cb, Open) =>
      state = Pausing (List (cb))
    case (cb, Pausing (cbs)) =>
      state = Pausing (cb :: cbs)
    case (cb, Closing (cbs)) =>
      state = Closing (cb :: cbs)
    case (cb, _) =>
      scheduler.pass (cb, ())
  }

  def resume(): Unit = execute {
    case Paused if !enrolled && receiver.isDefined =>
      state = Open
      enrolled = true
      dispatcher.receive (dispatch)
    case Paused =>
      state = Open
    case _ =>
      ()
  }

  def close(): Async [Unit] = async {
    case (cb, Closed) =>
      scheduler.pass (cb, ())
    case (cb, Pausing (cbs)) =>
      state = Closing (cb :: cbs)
    case (cb, Closing (cbs)) =>
      state = Closing (cb :: cbs)
    case (cb, _) if receiver.isDefined =>
      state = Closed
      scheduler.pass (cb, ())
    case (cb, _) =>
      state = Closing (List (cb))
  }}
