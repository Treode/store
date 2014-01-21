package com.treode.disk

import com.treode.async.Callback
import com.treode.pickle.Pickler

class RecordDescriptor [R] (id: TypeId, prec: Pickler [R]) {

  private [disk] def register (records: RecordRegistry) (f: R => Any): Unit =
    records.register (prec, id) (f)

  def register (f: R => Any) (implicit disks: Disks): Unit =
    disks.register (prec, id) (f)

  private [disk] def apply (log: LogDispatcher) (entry: R) (cb: Callback [Unit]): Unit =
    log.record (prec, id, entry, cb)

  def apply (entry: R) (cb: Callback [Unit]) (implicit disks: Disks): Unit =
    disks.record (prec, id, entry, cb)
}
