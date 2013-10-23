package com.treode.concurrent

import java.util

class Mailbox [M] (scheduler: Scheduler) {

  private[this] val messages = new util.ArrayDeque [M]
  private[this] val receivers = new util.ArrayDeque [M => Any]

  private def execute (receiver: M => Any, message: M): Unit =
    scheduler.execute (receiver (message))

  def send (message: M): Unit = synchronized {
    if (receivers.isEmpty)
      messages.add (message)
    else
      execute (receivers.remove(), message)
  }

  def receive (receiver: M => Any): Unit = synchronized {
    if (messages.isEmpty)
      receivers.add (receiver)
    else
      execute (receiver, messages.remove())
  }}
