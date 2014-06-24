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

import java.util.concurrent.ConcurrentHashMap
import com.treode.async.AsyncIterator
import com.treode.store.locks.LockSet
import com.treode.store.tier.{TierMedic, TierTable}

package atomic {

  private sealed abstract class PrepareResult

  private object PrepareResult {
    case class Prepared (vt: TxClock, locks: LockSet) extends PrepareResult
    case class Collided (ks: Seq [Int]) extends PrepareResult
    case object Stale extends PrepareResult
  }}

package object atomic {

  private [atomic] type TablesMap = ConcurrentHashMap [TableId, TierTable]
  private [atomic] type TableMedicsMap = ConcurrentHashMap [TableId, TierMedic]
  private [atomic] type WritersMap = ConcurrentHashMap [TxId, WriteDeputy]
  private [atomic] type WriterMedicsMap = ConcurrentHashMap [TxId, Medic]

  private [atomic] def newTablesMap = new ConcurrentHashMap [TableId, TierTable]
  private [atomic] def newTableMedicsMap = new ConcurrentHashMap [TableId, TierMedic]
  private [atomic] def newWritersMap = new ConcurrentHashMap [TxId, WriteDeputy]
  private [atomic] def newWriterMedicsMap = new ConcurrentHashMap [TxId, Medic]

  private [atomic] def locate (atlas: Atlas, table: TableId, key: Bytes): Cohort =
    atlas.locate (Cell.locator, (table, key))

  private [atomic] def place (atlas: Atlas, table: TableId, key: Bytes): Int =
    atlas.place (Cell.locator, (table, key))
}
