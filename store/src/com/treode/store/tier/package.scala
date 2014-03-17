package com.treode.store

import java.util.concurrent.ConcurrentSkipListMap
import com.treode.async.AsyncIterator

package object tier {

  private [tier] type MemTier =
    ConcurrentSkipListMap [Bytes, Option [Bytes]]

  private [tier] type TierCellIterator =
    AsyncIterator [TierCell]

  private [tier] val emptyMemTier =
    new ConcurrentSkipListMap [Bytes, Option [Bytes]] (Bytes)

  private [tier] def newMemTier =
    new ConcurrentSkipListMap [Bytes, Option [Bytes]] (Bytes)
}
