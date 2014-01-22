package com.treode.disk

import com.treode.async.Callback
import com.treode.pickle.Pickler

class RootDescriptor [B] (val id: TypeId, val pblk: Pickler [B]) {

  def recover (f: B => Any) (implicit disks: Disks): Unit =
    disks.recover (this) (f)

  def checkpoint (f: Callback [B] => Any) (implicit disks: Disks): Unit =
    disks.checkpoint (this) (f)
}
