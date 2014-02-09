package com.treode.disk

import com.treode.pickle.PicklerRegistry

import PicklerRegistry.FunctionTag

class ReloadRegistry {

  val loaders =
    PicklerRegistry [FunctionTag [Reload, Any]] ("ReloadRegistry")

  def reload [B] (desc: RootDescriptor [B]) (f: B => Reload => Any): Unit =
    PicklerRegistry.curried (loaders, desc.pblk, desc.id.id) (f)

  def pager = CheckpointRegistry.pager (loaders.pickler)
}
