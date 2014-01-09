package com.treode.store.disk2

import java.util.ArrayList
import java.util.concurrent.atomic.AtomicInteger
import com.treode.async.{Callback, Scheduler}

import LogEntry.Pending

private class LogDispatcher (scheduler: Scheduler) {

  private val dsp = new Dispatcher [Pending] (scheduler)

  def record (entry: LogEntry.Body, cb: Callback [Unit]) {
    val time = System.currentTimeMillis
    dsp.send (Pending (entry, time, cb))
  }

  def engage (writer: LogWriter): Unit =
    dsp.receive (writer.receiver)

  def replace (rejects: ArrayList [Pending]): Unit =
    dsp.replace (rejects)
}
