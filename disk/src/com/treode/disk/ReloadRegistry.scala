package com.treode.disk

import com.treode.async.Async
import com.treode.pickle.PicklerRegistry

import PicklerRegistry.{Tag, tag}

class ReloadRegistry {

  val loaders = PicklerRegistry [Tag] ("ReloadRegistry")

  def reload [B] (desc: RootDescriptor [B]) (f: B => Any): Unit =
    PicklerRegistry.action (loaders, desc.pblk, desc.id.id) (f)

  def pager = CheckpointRegistry.pager (loaders.pickler)
}
