package com.treode.disk

import com.treode.async.Callback
import com.treode.pickle.Pickler

class RootDescriptor [B] (val id: TypeId, val pblk: Pickler [B]) {

  def open (f: Recovery => Any) (implicit disks: Disks): Unit =
    disks.open (this) (f)

  def recover (recovery: Recovery) (f: (B, Callback [Unit]) => Any): Unit =
    recovery.recover (this) (f)

  def checkpoint (f: Callback [B] => Any) (implicit disks: Disks): Unit =
    disks.checkpoint (this) (f)

  override def toString = s"RootDescriptor($id)"
}
