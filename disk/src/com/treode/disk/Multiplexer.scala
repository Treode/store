package com.treode.disk

import scala.collection.mutable.UnrolledBuffer
import scala.reflect.ClassTag

import com.treode.async.{Async, Callback, Fiber, Scheduler}

class Multiplexer [M] (dispatcher: Dispatcher [M]) (
    implicit scheduler: Scheduler, mtag: ClassTag [M]) {

  private type R = (Long, UnrolledBuffer [M]) => Any

  private val fiber = new Fiber (scheduler)
  private var enrolled = false
  private var exclusive = false
  private var closed = false
  private var messages = new UnrolledBuffer [M]
  private var receivers = Option.empty [R]
  private var closer = Option.empty [Callback [Unit]]

  private def drain(): UnrolledBuffer [M] = {
    val t = messages
    messages = new UnrolledBuffer
    t
  }

  private def remove(): R = {
    val t = receivers.get
    receivers = Option.empty
    t
  }

  def deliver (receiver: R, batch: Long, messages: UnrolledBuffer [M]): Unit =
    scheduler.execute (receiver (batch, messages))

  private def _close() {
    scheduler.pass (closer.get, ())
    closer = Option.empty
  }

  def send (message: M): Unit = fiber.execute {
    require (!closed, "Multiplexer has been closed.")
    if (!receivers.isEmpty) {
      exclusive = true
      deliver (remove(), 0L, UnrolledBuffer (message))
    } else {
      messages += message
    }}

  def close(): Async [Unit] = fiber.async { cb =>
    require (!closed, "Multiplexer has been closed.")
    closed = true
    if (!receivers.isEmpty) {
      receivers = Option.empty
      scheduler.pass (cb, ())
    } else {
      closer = Some (cb)
    }}

  def isClosed: Boolean = closed

  private def dispatch (batch: Long, messages: UnrolledBuffer [M]): Unit = fiber.execute {
    enrolled = false
    if (!receivers.isEmpty) {
      exclusive = false
      deliver (remove(), batch, messages)
    } else {
      dispatcher.replace (messages)
    }}

  private val _dispatch: R = (dispatch _)

  def receive (receiver: R): Unit = fiber.execute {
    require (receivers.isEmpty)
    if (!messages.isEmpty) {
      exclusive = true
      deliver (receiver, 0L, drain())
    } else if (!closer.isEmpty) {
      _close()
    } else {
      receivers = Some (receiver)
      if (!closed && !enrolled) {
        enrolled = true
        dispatcher.receive (_dispatch)
      }}}

  def replace (rejects: UnrolledBuffer [M]) {
    if (exclusive) {
      rejects.concat (messages)
      messages = rejects
    } else {
      dispatcher.replace (rejects)
    }}
}
