package com.treode.disk

import com.treode.async.Async
import com.treode.pickle.Pickler

class RootDescriptor [B] private (val id: TypeId, val pblk: Pickler [B]) {

  def reload (f: B => Any) (implicit recovery: Disks.Recovery): Unit =
    recovery.reload (this) (f)

  def checkpoint (f: => Async [B]) (implicit launch: Disks.Launch): Unit =
    launch.checkpoint (this) (f)

  override def toString = s"RootDescriptor($id)"
}

object RootDescriptor {

  def apply [B] (id: TypeId, pblk: Pickler [B]): RootDescriptor [B] =
    new RootDescriptor (id, pblk)
}
