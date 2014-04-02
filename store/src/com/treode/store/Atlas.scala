package com.treode.store

import com.treode.async.Async
import com.treode.cluster.ReplyTracker
import com.treode.store.atlas.AtlasKit

private trait Atlas {

  def locate (id: Int): Cohort
}

private object Atlas {

  def recover (recovery: Catalogs.Recovery): Atlas =
   AtlasKit.recover (recovery)
}
