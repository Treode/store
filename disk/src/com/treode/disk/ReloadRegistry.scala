package com.treode.disk

import com.treode.pickle.PicklerRegistry

import PicklerRegistry.TaggedFunction

class ReloadRegistry {

  val loaders = PicklerRegistry [TaggedFunction [Reload, Any]] ()

  def reload [B] (desc: RootDescriptor [B]) (f: B => Reload => Any): Unit =
    PicklerRegistry.curried (loaders, desc.pblk, desc.id.id) (f)

  def pickler = loaders.pickler
}
