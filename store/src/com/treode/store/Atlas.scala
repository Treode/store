package com.treode.store

import com.treode.async.Async
import com.treode.store.atlas.AtlasKit

private trait Atlas {

}

private object Atlas {

  trait Recovery {

    def launch(): Async [Atlas]
  }

  def recover (recovery: Catalogs.Recovery): Recovery =
   AtlasKit.recover (recovery)
}
