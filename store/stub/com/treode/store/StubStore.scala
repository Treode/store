package com.treode.store

import java.util.concurrent.ConcurrentSkipListMap
import scala.collection.JavaConversions

import com.treode.async.{Async, AsyncIterator, Scheduler}
import com.treode.async.implicits._
import com.treode.async.stubs.StubScheduler
import com.treode.store.locks.LockSpace

import Async.guard
import JavaConversions._

class StubStore (implicit scheduler: Scheduler) extends Store {

  // The stub uses only the lockSpaceBits.
  private implicit val config =
    StoreConfig (Epoch.zero, 0.01, 4, Int.MaxValue, Int.MaxValue, Int.MaxValue)

  private val space = new LockSpace
  private val data = new ConcurrentSkipListMap [StubKey, Option [Bytes]]

  private def get (table: TableId, key: Bytes, time: TxClock): Value = {
    val limit = StubKey (table, key, TxClock.zero)
    var entry = data.ceilingEntry (StubKey (table, key, time))
    if (entry != null && entry.getKey <= limit)
      Value (entry.getKey.time, entry.getValue)
    else
      Value (TxClock.zero, None)
  }

  def read (rt: TxClock, ops: ReadOp*): Async [Seq [Value]] = {
    val ids = ops map (op => (op.table, op.key).hashCode)
    for {
      _ <- space.read (rt, ids)
    } yield {
      ops.map (op => get (op.table, op.key, rt))
    }}

  private def put (table: TableId, key: Bytes, time: TxClock, value: Bytes): Unit =
    data.put (StubKey (table, key, time), Some (value))

  private def delete (table: TableId, key: Bytes, time: TxClock): Unit =
    data.put (StubKey (table, key, time), None)

  private def collided (op: WriteOp, value: Value): Boolean =
    op.isInstanceOf [WriteOp.Create] && value.value.isDefined

  private def prepare (ct: TxClock, op: WriteOp): (Boolean, TxClock) = {
    val value = get (op.table, op.key, TxClock.max)
    (collided (op, value), value.time)
  }

  private def commit (wt: TxClock, ops: Seq [WriteOp]) {
    import WriteOp._
    for (op <- ops) yield {
      op match {
        case op: Create => put (op.table, op.key, wt, op.value)
        case op: Hold   => ()
        case op: Update => put (op.table, op.key, wt, op.value)
        case op: Delete => delete (op.table, op.key, wt)
      }}}

  def write (xid: TxId, ct: TxClock, ops: WriteOp*): Async [TxClock] =
    guard {
      import TxClock._
      val ids = ops map (op => (op.table, op.key).hashCode)
      for {
        locks <- space.write (ct, ids)
      } yield {
        try {
          val results = ops.map (op => prepare (ct, op))
          val collisions = for (((c, t), i) <- results.zipWithIndex; if c) yield i
          val vt = results.filterNot (_._1) .map (_._2) .fold (TxClock.zero) (TxClock.max _)
          if (ct < vt) {
            throw new StaleException
          } else if (!collisions.isEmpty) {
            throw new CollisionException (collisions)
          } else {
            val wt = TxClock.max (vt, locks.ft) + 1
            commit (wt, ops)
            wt
          }
        } finally {
          locks.release()
        }}}

  def scan (table: TableId, key: Bytes, time: TxClock): AsyncIterator [Cell] = {
    val entries = data .tailMap (StubKey (table, key, time)) .takeWhile (_._1.table == table)
    for ((key, value) <- entries.async)
      yield Cell (key.key, key.time, value)
  }

  def scan (table: TableId): Seq [Cell] =
    for ((key, value) <- data.toSeq; if key.table == table)
      yield Cell (key.key, key.time, value)
}

object StubStore {

  def apply () (implicit scheduler: Scheduler): StubStore =
    new StubStore
}
