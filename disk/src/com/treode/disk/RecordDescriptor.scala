package com.treode.disk

import com.treode.async.Callback
import com.treode.pickle.Pickler

class RecordDescriptor [R] (val id: TypeId, val prec: Pickler [R]) {

  def apply (entry: R) (cb: Callback [Unit]) (implicit disks: Disks): Unit =
    disks.record (this, entry, cb)

  def replay (f: R => Any) (implicit recovery: Recovery): Unit =
    recovery.replay (this) (f)

  override def toString = s"RecordDescriptor($id)"
}
