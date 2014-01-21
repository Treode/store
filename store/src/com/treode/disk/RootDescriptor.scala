package com.treode.disk

import com.treode.async.Callback
import com.treode.pickle.Pickler

class RootDescriptor [B] (id: TypeId, pblk: Pickler [B]) {

  def checkpoint (f: => B) (implicit disks: Disks): Unit = ???

  def recover (f: B => Any) (implicit disks: Disks): Unit = ???
}
