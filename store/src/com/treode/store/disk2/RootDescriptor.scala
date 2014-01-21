package com.treode.store.disk2

import com.treode.async.Callback
import com.treode.pickle.Pickler

class RootDescriptor [B <: AnyRef] (id: TypeId, pblk: Pickler [B]) {

  private [disk2] def checkpoint (roots: RootRegistry) (f: Callback [B] => Any): Unit =
    roots.checkpoint (pblk, id) (f)

  private [disk2] def recover (roots: RootRegistry) (f: B => Any): Unit =
    roots.recover (pblk, id) (f)

  def checkpoint (f: => B) (implicit disks: Disks): Unit = ???

  def recover (f: B => Any) (implicit disks: Disks): Unit = ???
}
