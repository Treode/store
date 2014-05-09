package com.treode.disk

import scala.collection.mutable.UnrolledBuffer
import scala.reflect.ClassTag

import com.treode.async.{Async, Callback, Fiber, Scheduler}

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
  case class Pausing (cb: Callback [Unit]) extends State
  case class Closing (cb: Callback [Unit]) extends State

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

  def send (message: M): Unit = execute {
    case Closing (_) =>
      throw closedException
    case Closed =>
      throw closedException
    case _ if receiver.isDefined =>
      deliver (remove(), true, 0L, singleton (message))
    case _ =>
      messages += message
  }

  private def _dispatch (batch: Long, messages: UnrolledBuffer [M]): Unit = execute {
    case Open if receiver.isDefined =>
      assert (enrolled)
      enrolled = false
      deliver (remove(), false, batch, messages)
    case _ =>
      assert (enrolled)
      enrolled = false
      dispatcher.replace (messages)
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
    case Pausing (cb) =>
      state = Paused
      add (receiver)
      scheduler.pass (cb, ())
    case Closed =>
      ()
    case Closing (cb) =>
      state = Closed
      scheduler.pass (cb, ())
  }

  def replace (rejects: UnrolledBuffer [M]): Unit = execute {
    case _ if exclusive =>
      messages = rejects.concat (messages)
    case _ =>
      assert (!enrolled)
      dispatcher.replace (rejects)
  }

  def pause(): Async [Unit] = async {
    case (cb, Open) if receiver.isDefined =>
      state = Paused
      scheduler.pass (cb, ())
    case (cb, Open) =>
      state = Pausing (cb)
    case (cb1, Closing (cb2)) =>
      state = Closing (Callback.fanout (Seq (cb1, cb2)))
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
      scheduler.fail (cb, closedException)
    case (cb, Closing (_)) =>
      scheduler.fail (cb, closedException)
    case (cb1, Pausing (cb2)) =>
      state = Closing (Callback.fanout (Seq (cb1, cb2)))
    case (cb, _) if receiver.isDefined =>
      state = Closed
      scheduler.pass (cb, ())
    case (cb, _) =>
      state = Closing (cb)
  }}
