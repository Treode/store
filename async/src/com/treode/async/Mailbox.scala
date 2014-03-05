package com.treode.async

import java.util

class Mailbox [M] {

  private[this] val messages = new util.ArrayDeque [M]
  private[this] val receivers = new util.ArrayDeque [Callback [M]]

  def send (message: M): Unit = synchronized {
    if (receivers.isEmpty)
      messages.add (message)
    else
      receivers .remove() .pass (message)
  }

  private def receive (receiver: Callback [M]): Unit = synchronized {
    if (messages.isEmpty)
      receivers.add (receiver)
    else
      receiver.pass (messages.remove())
  }

  def receive(): Async [M] =
    Async.async (receive (_))
}
