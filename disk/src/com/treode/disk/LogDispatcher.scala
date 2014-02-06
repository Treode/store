package com.treode.disk

import java.util.ArrayList
import com.treode.async.{Callback, Scheduler}

private class LogDispatcher (implicit scheduler: Scheduler) {

  private val dsp = new Dispatcher [PickledRecord] (scheduler)

  def record [R] (desc: RecordDescriptor [R], entry: R, cb: Callback [Unit]): Unit =
    dsp.send (PickledRecord (desc, entry, cb))

  def record (disk: Int, entry: RecordHeader, cb: Callback [Unit]): Unit =
    dsp.send (PickledRecord (disk, entry, cb))

  def engage (writer: LogWriter): Unit =
    dsp.receive (writer.receiver)

  def replace (rejects: ArrayList [PickledRecord]): Unit =
    dsp.replace (rejects)
}
