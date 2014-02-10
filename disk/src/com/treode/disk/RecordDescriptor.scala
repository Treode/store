package com.treode.disk

import com.treode.async.Callback
import com.treode.pickle.Pickler

class RecordDescriptor [R] private (val id: TypeId, val prec: Pickler [R]) {

  def record (entry: R) (cb: Callback [Unit]) (implicit disks: Disks): Unit =
    disks.record (this, entry, cb)

  def replay (f: R => Any) (implicit recovery: Recovery): Unit =
    recovery.replay (this) (f)

  override def toString = s"RecordDescriptor($id)"
}

object RecordDescriptor {

  def apply [R] (id: TypeId, prec: Pickler [R]): RecordDescriptor [R] =
    new RecordDescriptor (id, prec)
}
