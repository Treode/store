package com.treode.store.atomic

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions

import com.treode.async.{Async, AsyncIterator, Callback, Latch}
import com.treode.async.implicits._
import com.treode.async.misc.materialize
import com.treode.disk.{Disk, ObjectId, PageHandler, Position, RecordDescriptor}
import com.treode.store._
import com.treode.store.locks.LockSpace
import com.treode.store.tier.{TierDescriptor, TierMedic, TierTable}

import Async.{async, guard, supply, when}
import JavaConversions._

private class TimedStore (kit: AtomicKit) extends PageHandler [Long] {
  import kit.{config, disks, library, scheduler}

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
    var table = tables.get (id)
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
        for ((op, i) <- ops.zipWithIndex.latch.seq)
          getTable (op.table) .get (op.key, rt)
    } yield {
      for (Cell (_, time, value) <- cells) yield Value (time, value)
    }
  }

  private def collided (op: WriteOp, cell: Cell): Boolean =
    op.isInstanceOf [WriteOp.Create] && cell.value.isDefined

  private def prepare (ct: TxClock, op: WriteOp): Async [(Boolean, TxClock)] = {
    for (cell <- getTable (op.table) .get (op.key, TxClock.MaxValue))
      yield (collided (op, cell), cell.time)
  }

  def prepare (ct: TxClock, ops: Seq [WriteOp]): Async [PrepareResult] = {
    import PrepareResult._
    require (!ops.isEmpty, "Prepare needs at least one operation")
    val ids = ops map (op => (op.table, op.key).hashCode)
    for {
      locks <- space.write (ct, ids)
      results <-
        for ((op, i) <- ops.zipWithIndex.latch.seq)
          prepare (ct, op)
    } yield {
      val collisions = for (((c, t), i) <- results.zipWithIndex; if c) yield i
      val vt = results.filterNot (_._1) .map (_._2) .fold (TxClock.MinValue) (TxClock.max _)
      if (ct < vt) {
        locks.release()
        Stale
      } else if (!collisions.isEmpty) {
        locks.release()
        Collided (collisions)
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

  def scan (table: TableId, start: Bound [Key], window: TimeBounds, slice: Slice): CellIterator =
      getTable (table)
        .iterator (start, library.residents)
        .slice (table, slice)
        .window (window)

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
        _ <- TimedStore.checkpoint.record (id, meta)
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
    for (e <- tables.latch.unit)
      checkpoint (e.getKey, e.getValue)
  }

  def recover (ts: Seq [(TableId, TierTable)]) {
    for ((id, t) <- ts)
      tables.put (id, t)
  }}

private object TimedStore {

  val table = TierDescriptor (0xB500D51FACAEA961L) { (residents, id, cell) =>
    residents.contains (Cell.locator, (id, cell.key))
  }

  val receive = {
    import AtomicPicklers._
    RecordDescriptor (0xBFD53CF2C3C0F5CAL, tuple (tableId, ulong, seq (cell)))
  }

  val checkpoint = {
    import AtomicPicklers._
    RecordDescriptor (0x1DB0E46F7FD15C5DL, tuple (tableId, tierMeta))
  }}
