package com.treode.store.atomic

import java.util.concurrent.ConcurrentHashMap

import com.treode.async.{Async, AsyncConversions, Callback, Latch}
import com.treode.async.misc.materialize
import com.treode.disk.{Disks, ObjectId, PageHandler, Position, RecordDescriptor}
import com.treode.store.{ReadOp, TableId, TxClock, TxId, Value, WriteOp}
import com.treode.store.locks.LockSpace
import com.treode.store.tier.{TierMedic, TierTable}

import Async.{async, guard, supply}
import AsyncConversions._

private class TimedStore (kit: AtomicKit) extends PageHandler [Long] {
  import kit.{config, disks, scheduler}

  val space = new LockSpace
  val tables = newTablesMap

  private def getTable (id: TableId): TimedTable = {
    var t0 = tables.get (id)
    if (t0 != null)
      return t0
    val t1 = TimedTable (id)
    t0 = tables.putIfAbsent (id, t1)
    if (t0 != null)
      return t0
    t1
  }

  def read (rt: TxClock, ops: Seq [ReadOp]): Async [Seq [Value]] = {
    require (!ops.isEmpty, "Read needs at least one operation")
    val ids = ops map (op => (op.table, op.key).hashCode)
    for {
      _ <- space.read (rt, ids)
      vs <- ops.latch.indexed { case (op, i) =>
          getTable (op.table) .get (op.key, rt)
      }
    } yield vs
  }

  private def collided (op: WriteOp, value: Value): Boolean =
    op.isInstanceOf [WriteOp.Create] && value.value.isDefined

  private def prepare (ct: TxClock, op: WriteOp): Async [(Boolean, TxClock)] = {
    for (value <- getTable (op.table) .get (op.key, TxClock.max))
      yield (collided (op, value), value.time)
  }

  def prepare (ct: TxClock, ops: Seq [WriteOp]): Async [PrepareResult] = {
    import PrepareResult._
    require (!ops.isEmpty, "Prepare needs at least one operation")
    val ids = ops map (op => (op.table, op.key).hashCode)
    for {
      locks <- space.write (ct, ids)
      results <- ops.latch.indexed {case (op, i) => prepare (ct, op)}
    } yield {
      val collisions = for (((c, t), i) <- results.zipWithIndex; if c) yield i
      val vt = results.filterNot (_._1) .map (_._2) .fold (TxClock.zero) (TxClock.max _)
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

  def recover (ts: Seq [(TableId, TimedTable)]) {
    for ((id, t) <-ts)
      tables.put (id, t)
  }

  def probe (obj: ObjectId, groups: Set [Long]): Async [Set [Long]] =
    supply (getTable (obj.id) .probe (groups))

  def compact (obj: ObjectId, groups: Set [Long]): Async [Unit] =
    guard {
      val id = TableId (obj.id)
      for {
        meta <- getTable (id) .compact (groups)
        _ <- TimedStore.checkpoint.record (id, meta)
      } yield ()
    }

  private def checkpoint (id: TableId, table: TimedTable): Async [Unit] =
    table.checkpoint() .flatMap (TimedStore.checkpoint.record (id, _))

  def checkpoint(): Async [Unit] = {
    val tables = materialize (this.tables.entrySet)
    tables.latch.unit (e => checkpoint (e.getKey, e.getValue))
  }}

private object TimedStore {

  val checkpoint = {
    import AtomicPicklers._
    RecordDescriptor (0x1DB0E46F7FD15C5DL, tuple (tableId, tierMeta))
  }}
