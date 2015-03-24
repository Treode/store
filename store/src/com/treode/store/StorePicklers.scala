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

import com.treode.cluster.{CellId, HostId, PortId}
import com.treode.disk.{ObjectId, Position, TypeId}
import com.treode.pickle.{Pickler, Picklers}
import com.treode.store.tier.TierTable
import org.joda.time.Instant

private trait StorePicklers extends Picklers {

  lazy val instant = wrap (ulong) build (new Instant (_)) inspect (_.getMillis)

  lazy val sysid = tuple (hostId, cellId)

  def bound [A] (pa: Pickler [A]) = Bound.pickler (pa)

  def atlas = Atlas.pickler
  def ballotNumber = BallotNumber.pickler
  def batch = Batch.pickler
  def bytes = Bytes.pickler
  def catId = CatalogId.pickler
  def cell = Cell.pickler
  def cellId = CellId.pickler
  def cohort = Cohort.pickler
  def hostId = HostId.pickler
  def key = Key.pickler
  def portId = PortId.pickler
  def pos = Position.pickler
  def readOp = ReadOp.pickler
  def residents = Residents.pickler
  def slice = Slice.pickler
  def tableId = TableId.pickler
  def tierCheckpoint = TierTable.Checkpoint.pickler
  def tierCompaction = TierTable.Compaction.pickler
  def txClock = TxClock.pickler
  def txId = TxId.pickler
  def txStatus = TxStatus.pickler
  def value = Value.pickler
  def window = Window.pickler
  def writeOp = WriteOp.pickler
}

private object StorePicklers extends StorePicklers
