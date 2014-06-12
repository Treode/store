package com.treode.disk

import com.treode.async.{Async, Callback}
import com.treode.pickle.Pickler

class RecordDescriptor [R] private (val id: TypeId, val prec: Pickler [R]) {

  def record (entry: R) (implicit disk: Disk): Async [Unit] =
    disk.record (this, entry)

  def replay (f: R => Any) (implicit recovery: Disk.Recovery): Unit =
    recovery.replay (this) (f)

  override def toString = s"RecordDescriptor($id)"
}

object RecordDescriptor {

  def apply [R] (id: TypeId, prec: Pickler [R]): RecordDescriptor [R] =
    new RecordDescriptor (id, prec)
}
