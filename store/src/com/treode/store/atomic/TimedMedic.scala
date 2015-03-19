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

package com.treode.store.atomic

import com.treode.async.Async
import com.treode.async.misc.materialize
import com.treode.disk.DiskLaunch
import com.treode.store.{Cell, TableId, TxClock, WriteOp}
import com.treode.store.tier.{TierMedic, TierTable}

import Async.guard

private class TimedMedic (kit: RecoveryKit) {
  import kit.{config, scheduler}

  val tables = newTableMedicsMap

  def get (id: TableId): TierMedic = {
    val m1 = TierMedic (TimedStore.table, id.id)
    val m0 = tables.putIfAbsent (id, m1)
    if (m0 == null) m1 else m0
  }

  def commit (gens: Seq [Long], wt: TxClock, ops: Seq [WriteOp]) {
    import WriteOp._
    require (gens.length == ops.length)
    for ((gen, op) <- gens zip ops) {
      val t = get (op.table)
      op match {
        case op: Create => t.put (gen, op.key, wt, op.value)
        case op: Hold   => 0
        case op: Update => t.put (gen, op.key, wt, op.value)
        case op: Delete => t.delete (gen, op.key, wt)
      }}}

  def receive (id: TableId, gen: Long, novel: Seq [Cell]): Unit =
    get (id) .receive (gen, novel)

  def compact (id: TableId, meta: TierTable.Compaction): Unit =
    get (id) .compact (meta)

  def checkpoint (id: TableId, meta: TierTable.Checkpoint): Unit =
    get (id) .checkpoint (meta)

  def close() (implicit launch: DiskLaunch): Seq [(TableId, TierTable)] = {
    materialize (tables.entrySet) map { entry =>
      val id = entry.getKey
      val t = entry.getValue.close()
      (id, t)
    }}}
