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

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions

import com.treode.async.{Async, BatchIterator, Callback, Latch}
import com.treode.async.implicits._
import com.treode.async.misc.materialize
import com.treode.disk.{Disk, ObjectId, PageHandler, Position, RecordDescriptor}
import com.treode.store._
import com.treode.store.locks.LockSpace
import com.treode.store.tier.{TierDescriptor, TierMedic, TierTable}

import Async.{async, guard, supply, when}
import JavaConversions._

private class TimedStore (kit: AtomicKit) extends PageHandler [Long] {
  import kit.{config, disk, library, scheduler}

  val space = new LockSpace
  val tables = newTablesMap

  private def getTable (id: TableId): TierTable = {
    var t0 = tables.get (id)
    if (t0 != null) return t0
    val t1 = TierTable (TimedStore.table, id.id)
    t0 = tables.putIfAbsent (id, t1)
    if (t0 != null) return t0
    t1
  }

  def ceiling (id: TableId): Option [TierTable] = {
    val table = tables.get (id)
    if (table != null) return Some (table)
    val rest = tables.entrySet.filter (_.getKey > id)
    if (rest.isEmpty) return None
    Some (rest.minBy (_.getKey) .getValue)
  }

  def read (rt: TxClock, ops: Seq [ReadOp]): Async [Seq [Value]] = {
    require (!ops.isEmpty, "Read needs at least one operation")
    val ids = ops map (op => (op.table, op.key).hashCode)
    for {
      _ <- space.read (rt, ids)
      cells <-
        for ((op, i) <- ops.indexed)
          yield getTable (op.table) .get (op.key, rt)
    } yield {
      for (Cell (_, time, value) <- cells) yield Value (time, value)
    }
  }

  private def collided (ops: Seq [WriteOp], cells: Seq [Cell]): Seq [Int] =
    for {
      ((op, cell), i) <- ops.zip (cells) .zipWithIndex
      if op.isInstanceOf [WriteOp.Create] && cell.value.isDefined
    } yield i

  def prepare (ct: TxClock, ops: Seq [WriteOp]): Async [PrepareResult] = {
    import PrepareResult._
    require (!ops.isEmpty, "Prepare needs at least one operation")
    val ids = ops map (op => (op.table, op.key).hashCode)
    for {
      locks <- space.write (ct, ids)
      cells <-
        for ((op, i) <- ops.indexed)
          yield getTable (op.table) .get (op.key, TxClock.MaxValue)
    } yield {
      val vt = cells.map (_.time) .fold (TxClock.MinValue) (TxClock.max _)
      val collisions = collided (ops, cells)
      if (!collisions.isEmpty) {
        locks.release()
        Collided (collisions)
      } else if (ct < vt) {
        locks.release()
        Stale
      } else {
        Prepared (TxClock.max (vt, locks.ft), locks)
      }}}

  def commit (wt: TxClock, ops: Seq [WriteOp]): Seq [Long] = {
    import WriteOp._
    require (!ops.isEmpty, "Commit needs at least one operation")
    for (op <- ops; t = getTable (op.table)) yield {
      op match {
        case op: Create => t.put (op.key, wt, op.value)
        case op: Hold   => 0
        case op: Update => t.put (op.key, wt, op.value)
        case op: Delete => t.delete (op.key, wt)
      }}}

  def scan (table: TableId, start: Bound [Key], window: Window, slice: Slice): CellIterator =
    BatchIterator.make {
      for {
        _ <- space.scan (window.later.bound)
      } yield {
        getTable (table) .iterator (start, window, slice, library.residents)
      }}

  def receive (table: TableId, cells: Seq [Cell]): Async [Unit] = {
    val (gen, novel) = getTable (table) .receive (cells)
    when (!novel.isEmpty) (TimedStore.receive.record (table, gen, novel))
  }

  def probe (obj: ObjectId, groups: Set [Long]): Async [Set [Long]] =
    guard {
      getTable (obj.id) .probe (groups)
    }

  def compact() {
    for (table <- tables.values)
      table.compact()
  }

  def compact (obj: ObjectId, groups: Set [Long]): Async [Unit] =
    guard {
      val id = TableId (obj.id)
      val residents = library.residents
      for {
        meta <- getTable (id) .compact (groups, residents)
        _ <- when (meta.isDefined) (TimedStore.compact.record (id, meta.get))
      } yield ()
    }

  private def checkpoint (id: TableId, table: TierTable): Async [Unit] = {
    val residents = library.residents
    for {
      meta <- table.checkpoint (residents)
      _ <- TimedStore.checkpoint.record (id, meta)
    } yield ()
  }

  def checkpoint(): Async [Unit] = {
    val tables = materialize (this.tables.entrySet)
    for (e <- tables.latch)
      checkpoint (e.getKey, e.getValue)
  }

  def recover (ts: Seq [(TableId, TierTable)]) {
    for ((id, t) <- ts)
      tables.put (id, t)
  }

  def digest: Seq [TableDigest] =
    tables.values.map (_.digest) .toSeq
}

private object TimedStore {

  val table = TierDescriptor (0xB500D51FACAEA961L) { (residents, id, cell) =>
    residents.contains (Cell.locator, (id, cell.key))
  }

  val receive = {
    import AtomicPicklers._
    RecordDescriptor (0xBFD53CF2C3C0F5CAL, tuple (tableId, ulong, seq (cell)))
  }

  val compact = {
    import AtomicPicklers._
    RecordDescriptor (0x4B5391ACA26DD90BL, tuple (tableId, tierCompaction))
  }

  val checkpointV0 = {
    import AtomicPicklers._
    RecordDescriptor (0x1DB0E46F7FD15C5DL, tuple (tableId, tierMeta))
  }

  val checkpoint = {
    import AtomicPicklers._
    RecordDescriptor (0xDEA0D8CAC3405C79L, tuple (tableId, tierCheckpoint))
  }}
