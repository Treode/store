package com.treode.store

import java.util.{Map => JMap}
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}
import scala.collection.JavaConversions._

package object tier {

  private [tier] type MemTier =
    ConcurrentNavigableMap [Key, Option [Bytes]]

  private [tier] val emptyMemTier: MemTier =
    new ConcurrentSkipListMap [Key, Option [Bytes]] (Key)

  private [tier] def newMemTier: MemTier =
    new ConcurrentSkipListMap [Key, Option [Bytes]] (Key)

  private [tier] def memTierEntryToCell (entry: JMap.Entry [Key, Option [Bytes]]): Cell =
    Cell (entry.getKey.key, entry.getKey.time, entry.getValue)

  private [tier] def countMemTierKeys (tier: MemTier): Long = {
    var count = 0L
    var key = Bytes.empty
    for (k <- tier.keySet; if k.key != k) {
      key = k.key
      count += 1
    }
    count
  }

  private [tier] implicit class RichCellIterator (iter: CellIterator) {

    def clean (desc: TierDescriptor, id: TableId, residents: Residents) (
        implicit config: StoreConfig): CellIterator =
      iter.dedupe
          .retire (config.priorValueEpoch.limit)
          .filter (desc.residency (residents, id, _))
  }}
