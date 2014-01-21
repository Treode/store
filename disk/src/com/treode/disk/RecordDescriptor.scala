package com.treode.disk

import com.treode.async.Callback
import com.treode.pickle.Pickler

class RecordDescriptor [R] (val id: TypeId, val prec: Pickler [R]) {

  private [disk] def register (records: RecordRegistry) (f: R => Any): Unit =
    records.register (this) (f)

  def register (f: R => Any) (implicit disks: Disks): Unit =
    disks.register (this) (f)

  private [disk] def apply (log: LogDispatcher) (entry: R) (cb: Callback [Unit]): Unit =
    log.record (this, entry, cb)

  def apply (entry: R) (cb: Callback [Unit]) (implicit disks: Disks): Unit =
    disks.record (this, entry, cb)
}
