package com.treode.disk

import java.util.ArrayList
import com.treode.async.{Callback, Scheduler}
import com.treode.pickle.Pickler

private class LogDispatcher (scheduler: Scheduler) {

  private val dsp = new Dispatcher [PickledRecord] (scheduler)

  def record [R] (p: Pickler [R], id: TypeId, entry: R, cb: Callback [Unit]): Unit =
    dsp.send (PickledRecord (p, id, System.currentTimeMillis, entry, cb))

  def engage (writer: LogWriter): Unit =
    dsp.receive (writer.receiver)

  def replace (rejects: ArrayList [PickledRecord]): Unit =
    dsp.replace (rejects)
}
