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

import scala.util.Random

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.cluster.{Cluster, HostId, ReplyTracker}
import com.treode.disk.Disk
import com.treode.store._
import com.treode.store.paxos.Paxos
import com.treode.store.tier.TierTable

import Async.{async, supply}
import AtomicMover.Targets
import WriteDirector.deliberate

private class AtomicKit (implicit
    val random: Random,
    val scheduler: Scheduler,
    val cluster: Cluster,
    val disk: Disk,
    val library: Library,
    val paxos: Paxos,
    val config: StoreConfig
) extends Atomic {

  import library.{atlas, releaser}

  val tstore = new TimedStore (this)
  val reader = new ReadDeputy (this)
  val writers = new WriteDeputies (this)
  val scanner = new ScanDeputy (this)
  val mover = new AtomicMover (this)

  def read (rt: TxClock, ops: ReadOp*): Async [Seq [Value]] =
    releaser.join (ReadDirector.read (rt, ops, this))

  def write (xid: TxId, ct: TxClock, pt: TxClock, ops: WriteOp*): Async [TxClock] =
    releaser.join (WriteDirector.write (xid, ct, pt, ops, this))

  def write (xid: TxId, ct: TxClock, ops: WriteOp*): Async [TxClock] =
    write (xid, ct, TxClock.now, ops: _*)

  def status (xid: TxId): Async [TxStatus] =
    deliberate.propose (xid.id, xid.time, TxStatus.Aborted)

  def scan (table: TableId, start: Bound [Key], window: Window, slice: Slice, batch: Batch): CellIterator =
    ScanDirector.scan (table, start, window, slice, batch, this)

  def rebalance (atlas: Atlas): Async [Unit] = {
    val targets = Targets (atlas)
    for {
      _ <- mover.rebalance (targets)
    } yield {
      if (targets.isEmpty)
        tstore.compact()
    }}

  def tables: Async [Seq [TableDigest]] =
    supply (tstore.digest)
}
