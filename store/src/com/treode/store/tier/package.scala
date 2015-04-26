/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
        implicit config: StoreConfig): CellIterator =
      iter.dedupe
          .retire (config.retention.limit)
          .filter (desc.residency (residents, id, _))
  }}
