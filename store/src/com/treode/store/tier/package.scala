package com.treode.store

import java.util.{Map => JMap}
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}

package object tier {

  private [tier] type MemTier =
    ConcurrentNavigableMap [MemKey, Option [Bytes]]

  private [tier] val emptyMemTier: MemTier =
    new ConcurrentSkipListMap [MemKey, Option [Bytes]] (MemKey)

  private [tier] def newMemTier: MemTier =
    new ConcurrentSkipListMap [MemKey, Option [Bytes]] (MemKey)

  private [tier] def memTierEntryToCell (entry: JMap.Entry [MemKey, Option [Bytes]]): Cell =
    Cell (entry.getKey.key, entry.getKey.time, entry.getValue)

  private [tier] implicit class RichCellIterator (iter: CellIterator) {

    def dedupe: CellIterator =
      Filters.dedupe (iter)
  }}
