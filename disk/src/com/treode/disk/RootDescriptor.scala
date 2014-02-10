package com.treode.disk

import com.treode.async.Callback
import com.treode.pickle.Pickler

class RootDescriptor [B] private (val id: TypeId, val pblk: Pickler [B]) {

  def reload (f: B => Reload => Any) (implicit recovery: Recovery): Unit =
    recovery.reload (this) (f)

  def checkpoint (f: Callback [B] => Any) (implicit launch: Launch): Unit =
    launch.checkpoint (this) (f)

  override def toString = s"RootDescriptor($id)"
}

object RootDescriptor {

  def apply [B] (id: TypeId, pblk: Pickler [B]): RootDescriptor [B] =
    new RootDescriptor (id, pblk)
}
