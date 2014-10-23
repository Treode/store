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
import com.treode.disk.{Disk, TypeId}
import com.treode.store._

import Async.async
import TierTable.{Checkpoint, Compaction}

private [store] trait TierTable {

  def typ: TypeId

  def id: TableId

  def get (key: Bytes, time: TxClock): Async [Cell]

  def iterator (residents: Residents): CellIterator

  def iterator (start: Bound [Key], residents: Residents): CellIterator

  def put (key: Bytes, time: TxClock, value: Bytes): Long

  def delete (key: Bytes, time: TxClock): Long

  def receive (cells: Seq [Cell]): (Long, Seq [Cell])

  def probe (groups: Set [Long]): Async [Set [Long]]

  def compact()

  def compact (groups: Set [Long], residents: Residents): Async [Option [Compaction]]

  def checkpoint (residents: Residents): Async [Checkpoint]

  def digest: TableDigest
}

private [store] object TierTable {

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
