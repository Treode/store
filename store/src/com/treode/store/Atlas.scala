package com.treode.store

import com.treode.async.Async
import com.treode.cluster.ReplyTracker
import com.treode.store.atlas.AtlasKit

private trait Atlas {

  def locate (id: Int): ReplyTracker
}

private object Atlas {

  trait Recovery {

    def launch(): Async [Atlas]
  }

  def recover (recovery: Catalogs.Recovery): Recovery =
   AtlasKit.recover (recovery)
}
