package com.treode.store.disk2

import java.util.ArrayList
import java.util.concurrent.atomic.AtomicInteger
import com.treode.async.{Callback, Scheduler}
import com.treode.pickle.Pickler

private class LogDispatcher (scheduler: Scheduler) {

  private val dsp = new Dispatcher [PickledEntry] (scheduler)

  def record [R] (p: Pickler [R], id: TypeId, entry: R, cb: Callback [Unit]): Unit =
    dsp.send (PickledEntry (p, id, System.currentTimeMillis, entry, cb))

  def engage (writer: LogWriter): Unit =
    dsp.receive (writer.receiver)

  def replace (rejects: ArrayList [PickledEntry]): Unit =
    dsp.replace (rejects)
}
