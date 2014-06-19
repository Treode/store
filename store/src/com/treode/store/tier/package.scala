package com.treode.store

import java.util.{Map => JMap}
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}

package object tier {

  private [tier] type MemTier =
    ConcurrentNavigableMap [Key, Option [Bytes]]

  private [tier] val emptyMemTier: MemTier =
    new ConcurrentSkipListMap [Key, Option [Bytes]] (Key)

  private [tier] def newMemTier: MemTier =
    new ConcurrentSkipListMap [Key, Option [Bytes]] (Key)

  private [tier] def memTierEntryToCell (entry: JMap.Entry [Key, Option [Bytes]]): Cell =
    Cell (entry.getKey.key, entry.getKey.time, entry.getValue)

  private [tier] implicit class RichCellIterator (iter: CellIterator) {

    def clean (desc: TierDescriptor, id: TableId, residents: Residents) (
        implicit config: Store.Config): CellIterator =
      iter.dedupe
          .retire (config.priorValueEpoch.limit)
          .filter (desc.residency (residents, id, _))
  }}
