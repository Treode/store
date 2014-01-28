package com.treode.store

import java.util.concurrent.ConcurrentSkipListSet
import com.treode.async.AsyncIterator

package object simple {

  private [simple] type MemTier = ConcurrentSkipListSet [SimpleCell]
  private [simple] type SimpleIterator = AsyncIterator [SimpleCell]

  private [simple] val emptyMemTier = new ConcurrentSkipListSet [SimpleCell] (SimpleCell)
  private [simple] def newMemTier = new ConcurrentSkipListSet [SimpleCell] (SimpleCell)
}
