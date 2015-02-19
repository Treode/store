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

package com.treode.store.tier

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.disk.{Disk, GroupId, TypeId}
import com.treode.store._

import Async.async
import TierTable.{Checkpoint, Compaction}

/** A log structured merge tree.
  *
  * Two good foundational explanations are
  *
  *   - [[https://www.igvita.com/2012/02/06/sstable-and-log-structured-storage-leveldb/ https://www.igvita.com/2012/02/06/sstable-and-log-structured-storage-leveldb/]]
  *
  *   - [[http://wiki.apache.org/cassandra/MemtableSSTable Cassandra Wiki: MemtableSSTable]]
  *
  * Those references provide an rough understanding of the tiered table system. Details of this
  * implementation are adapted to support timestamped version of data, to account for the
  * rentention period, and to work with the cohort atlas and rebalancing.
  *
  * TierTables in TreodeDB '''do not''' record log entries. Instead, updates provide a key piece of
  * data (a Long), the client of the TierTable records that data in its log entry, and the client
  * uses TierMedic to replay its log entries. This mechanism slightly complicates the interface
  * for a TierTable, but it gives the client control of the transactional semantics.
  *
  * TierTables do not register themselves with the cleaner of the disk system. The client must
  * create a [[PageHandler]] to work with the cleaner, and it may pass probe and compact request
  * through to the tier table. The client may use this hook to implement whole table deletion.
  *
  * TierTables are aware of the cohort atlas. When iterating and compacting, they filter items that
  * may have resided on this node at one time, but should not any reside here any longer. Similarly,
  * TierTables are aware of retention. When iterating and compacting, they filter data which should
  * age out. In other words, after rebalancing data and after a retention period expires, they data
  * remains on disk. Iterators filter that data to give the appears of immediate impact. They data
  * is actually removed from disk during a compaction.
  */
private [store] trait TierTable {

  def typ: TypeId

  def id: TableId

  def get (key: Bytes, time: TxClock): Async [Cell]

  def iterator (residents: Residents): CellIterator

  def iterator (start: Bound [Key], residents: Residents): CellIterator

  def iterator (start: Bound [Key], window: Window, slice: Slice, residents: Residents): CellIterator

  /** Put a key.
    *
    * @return The generation. The client must log this key piece of data, and provide it to the
    * TierMedic during recovery.
    */
  def put (key: Bytes, time: TxClock, value: Bytes): Long

  /** Delete a key.
    *
    * @return The generation. The client must log this key piece of data, and provide it to the
    * TierMedic during recovery.
    */
  def delete (key: Bytes, time: TxClock): Long

  /** Receive cells from another host; part of rebalancing, that is part of moving data from an
    * old cohort of peers to a new cohort of peers when the atlas changes.
    *
    * @return The generation and new cells. The client must log these key pieces of data, and
    * provide it to the TierMedic during recovery. The client does not need to log all cells
    * received from the peer; it only needs to logged the new cells returned by this method.
    */
  def receive (cells: Seq [Cell]): (Long, Seq [Cell])

  def probe (groups: Set [GroupId]): Async [Set [GroupId]]

  def compact()

  def compact (groups: Set [GroupId], residents: Residents): Async [Option [Compaction]]

  def checkpoint (residents: Residents): Async [Checkpoint]

  def digest: TableDigest
}

private [store] object TierTable {

  /** Records a compaction in the write log.
    *
    * A compaction introduces a new tier that replaces multiple tiers from `tier.gen` (exclusive)
    * to `keep` (exclusive).
    */
  class Compaction (
    private [tier] val keep: Long,
    private [tier] val tier: Tier
  ) {

    override def toString: String =
      s"TierTable.Compaction($keep, $tier)"
  }

  object Compaction {

    val pickler = {
      import StorePicklers._
      wrap (long, Tier.pickler)
      .build (v => new Compaction (v._1, v._2))
      .inspect (v => (v.keep, v.tier))
    }}

  /** Records a checkpoint in the write log; specifically, records the list of tiers at
    * checkpoint.
    */
  class Checkpoint (
    private [tier] val gen: Long,
    private [tier] val tiers: Tiers
  ) {

    override def toString: String =
      s"TierTable.Checkpoint($gen, $tiers)"
  }

  object Checkpoint {

    val empty = new Checkpoint (0, Tiers.empty)

    val pickler = {
      import StorePicklers._
      wrap (long, Tiers.pickler)
      .build (v => new Checkpoint (v._1, v._2))
      .inspect (v => (v.gen, v.tiers))
    }}

  // TODO: Remove after release of 0.2.0.
  // Meta remains to replay logs from a pre 0.2.0 database.
  class Meta (
    private [tier] val gen: Long,
    private [tier] val tiers: Tiers
  ) {

    override def toString: String =
      s"TierTable.Meta($gen, $tiers)"
  }

  object Meta {

    val pickler = {
      import StorePicklers._
      wrap (ulong, Tiers.pickler)
      .build (v => new Meta (v._1, v._2))
      .inspect (v => (v.gen, v.tiers))
    }}

  def apply (desc: TierDescriptor, id: TableId) (
      implicit scheduler: Scheduler, disk: Disk, config: Store.Config): TierTable =
    SynthTable (desc, id)
}
