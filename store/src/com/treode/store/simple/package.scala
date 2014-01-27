package com.treode.store

import java.util.concurrent.ConcurrentSkipListSet
import com.treode.async.AsyncIterator
import com.treode.disk.Position

package object simple {

  private [simple] type MemTable = ConcurrentSkipListSet [SimpleCell]
  private [simple] type SimpleIterator = AsyncIterator [SimpleCell]
  private [simple] type Tiers = Array [Position]

  private [simple] def newMemTable = new ConcurrentSkipListSet [SimpleCell] (SimpleCell)
}
