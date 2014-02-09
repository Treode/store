package com.treode.store

import java.util.concurrent.ConcurrentSkipListMap
import com.treode.async.AsyncIterator

package object simple {

  private [simple] type MemTier =
    ConcurrentSkipListMap [Bytes, Option [Bytes]]

  private [simple] type SimpleIterator =
    AsyncIterator [SimpleCell]

  private [simple] val emptyMemTier =
    new ConcurrentSkipListMap [Bytes, Option [Bytes]] (Bytes)

  private [simple] def newMemTier =
    new ConcurrentSkipListMap [Bytes, Option [Bytes]] (Bytes)
}
