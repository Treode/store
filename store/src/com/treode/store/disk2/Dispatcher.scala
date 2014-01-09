package com.treode.store.disk2

import java.util.{ArrayDeque, ArrayList, Collection}
import com.treode.async.Scheduler

private class Dispatcher [M] (scheduler: Scheduler) {

  private type R = ArrayList [M] => Any

  private[this] var engaged = false
  private[this] var messages = new ArrayList [M]
  private[this] val receivers = new ArrayDeque [R]

  private def singleton (m: M): ArrayList [M] = {
    val a = new ArrayList [M] (1)
    a.add (m)
    a
  }

  private def drain(): ArrayList [M] = {
    val t = messages
    messages = new ArrayList [M]
    t
  }

  private def engage (receiver: R, messages: ArrayList [M]) {
    engaged = true
    scheduler.execute (receiver (messages))
  }

  def send (message: M): Unit = synchronized {
    if (engaged || receivers.isEmpty)
      messages.add (message)
    else
      engage (receivers.remove(), singleton (message))
  }

  def receive (receiver: R): Unit = synchronized {
    if (engaged || messages.isEmpty)
      receivers.add (receiver)
    else
      engage (receiver, drain())
  }

  def replace (rejects: ArrayList [M]): Unit = synchronized {
    if (receivers.isEmpty) {
      engaged = false
      if (!rejects.isEmpty) {
        rejects.addAll (messages)
        messages = rejects
      }
    } else if (messages.isEmpty) {
      engage (receivers.remove(), rejects)
    } else if (rejects.isEmpty) {
      val t = messages
      messages = rejects
      engage (receivers.remove(), t)
    } else {
      rejects.addAll (messages)
      messages.clear()
      engage (receivers.remove(), rejects)
    }}
}
