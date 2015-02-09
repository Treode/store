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

package com.treode.store.stubs

import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap}
import scala.collection.JavaConversions._

import com.treode.async.{Async, BatchIterator, Scheduler}
import com.treode.async.implicits._
import com.treode.async.stubs.StubScheduler
import com.treode.cluster.HostId
import com.treode.store._
import com.treode.store.locks.LockSpace

import Async.{async, guard}

class StubStore (implicit scheduler: Scheduler) extends Store {

  // The stub uses only the lockSpaceBits.
  private implicit val config = Store.Config.suggested.copy (lockSpaceBits = 4)

  private val space = new LockSpace
  private val xacts = new ConcurrentHashMap [TxId, TxStatus]
  private val data = new ConcurrentSkipListMap [StubKey, Option [Bytes]]

  private def get (table: TableId, key: Bytes, time: TxClock): Value = {
    val limit = StubKey (table, key, TxClock.MinValue)
    val entry = data.ceilingEntry (StubKey (table, key, time))
    if (entry != null && entry.getKey <= limit)
      Value (entry.getKey.time, entry.getValue)
    else
      Value (TxClock.MinValue, None)
  }

  def read (rt: TxClock, ops: ReadOp*): Async [Seq [Value]] =
    guard {
      val ids = ops map (op => (op.table, op.key).hashCode)
      for {
        _ <- space.read (rt, ids)
      } yield {
        ops.map (op => get (op.table, op.key, rt))
      }
    } .on (scheduler)

  private def put (table: TableId, key: Bytes, time: TxClock, value: Bytes): Unit =
    data.put (StubKey (table, key, time), Some (value))

  private def delete (table: TableId, key: Bytes, time: TxClock): Unit =
    data.put (StubKey (table, key, time), None)

  private def collided (op: WriteOp, value: Value): Boolean =
    op.isInstanceOf [WriteOp.Create] && value.value.isDefined

  private def prepare (ct: TxClock, op: WriteOp): (Boolean, TxClock) = {
    val value = get (op.table, op.key, TxClock.MaxValue)
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
      val ids = ops map (op => (op.table, op.key).hashCode)
      for {
        locks <- space.write (ct, ids)
      } yield {
        try {
          val results = ops.map (op => prepare (ct, op))
          val collisions = for (((c, t), i) <- results.zipWithIndex; if c) yield i
          val vt = results.filterNot (_._1) .map (_._2) .fold (TxClock.MinValue) (TxClock.max _)
          val wt = TxClock.max (vt, locks.ft) + 1
          if (ct < vt)
            throw new StaleException
          if (!collisions.isEmpty)
            throw new CollisionException (collisions)
          if (xacts.putIfAbsent (xid, TxStatus.Committed (wt)) != null)
            throw new TimeoutException
          commit (wt, ops)
          wt
        } finally {
          locks.release()
        }}
    } .on (scheduler)

  def status (xid: TxId): Async [TxStatus] =
    async [TxStatus] { cb =>
      val st = xacts.putIfAbsent (xid, TxStatus.Aborted)
      if (st == null)
        cb.pass (TxStatus.Aborted)
      else
        cb.pass (st)
    } .on (scheduler)

  def scan (table: TableId, start: Bound [Key], window: Window, slice: Slice, batch: Batch): BatchIterator [Cell] =
    BatchIterator.make {
      for {
        _ <- space.scan (window.later.bound)
      } yield {
        data
            .tailMap (StubKey (table, start.bound.key, start.bound.time), start.inclusive)
            .headMap (StubKey (table.id + 1, Bytes.empty, TxClock.MaxValue))
            .entrySet
            .batch
            .map (e => Cell (e.getKey.key, e.getKey.time, e.getValue))
            .slice (table, slice)
            .window (window)
      }}

  def scan (table: TableId): Seq [Cell] =
    for ((key, value) <- data.toSeq; if key.table == table)
      yield Cell (key.key, key.time, value)
}

object StubStore {

  def apply () (implicit scheduler: Scheduler): StubStore =
    new StubStore
}
