package com.treode.disk

import java.util.{ArrayDeque}
import scala.collection.mutable.UnrolledBuffer
import scala.reflect.ClassTag
import com.treode.async.{Fiber, Scheduler}

private class Dispatcher [M] (
    private var counter: Long
) (implicit
    scheduler: Scheduler,
    mtag: ClassTag [M]
) {

  private type R = (Long, UnrolledBuffer [M]) => Any

  private val fiber = new Fiber (scheduler)
  private var engaged = false
  private var messages = new UnrolledBuffer [M]

  val receivers = new ArrayDeque [R]

  private def singleton (m: M): UnrolledBuffer [M] =
    UnrolledBuffer (m)

  private def drain(): UnrolledBuffer [M] = {
    val t = messages
    messages = new UnrolledBuffer
    t
  }

  private def engage (receiver: R, messages: UnrolledBuffer [M]) {
    counter += 1
    engaged = true
    scheduler.execute (receiver (counter, messages))
  }

  def send (message: M): Unit = fiber.execute {
    if (engaged || receivers.isEmpty)
      messages += message
    else
      engage (receivers.remove(), singleton (message))
  }

  def receive (receiver: R): Unit = fiber.execute {
    if (engaged || messages.isEmpty)
      receivers.add (receiver)
    else
      engage (receiver, drain())
  }

  def replace (rejects: UnrolledBuffer [M]): Unit = fiber.execute {
    rejects.concat (messages)
    messages = rejects
    if (!messages.isEmpty && !receivers.isEmpty)
      engage (receivers.remove(), drain())
    else
      engaged = false
  }}
