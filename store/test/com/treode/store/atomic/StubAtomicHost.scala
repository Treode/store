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

import com.treode.async.{Async, BatchIterator, Scheduler}
import com.treode.async.implicits._
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.cluster.{Cluster, HostId, Peer}
import com.treode.cluster.stubs.{StubCluster, StubNetwork}
import com.treode.disk.Disk
import com.treode.disk.stubs.{StubDisk, StubDiskDrive}
import com.treode.store._
import com.treode.store.catalog.Catalogs
import com.treode.store.paxos.Paxos
import org.scalatest.Assertions

import Async.latch
import AtomicTestTools._

private class StubAtomicHost (
    val localId: HostId
) (implicit
    val random: Random,
    val scheduler: ChildScheduler,
    val cluster: StubCluster,
    val drive: StubDiskDrive,
    val disk: Disk,
    val library: Library,
    val catalogs: Catalogs,
    val paxos: Paxos,
    val atomic: AtomicKit
) extends StoreClusterChecks.Host {

  val librarian = Librarian { atlas =>
    latch (paxos.rebalance (atlas), atomic.rebalance (atlas)) .unit
  }

  cluster.startup()

  def shutdown(): Async [Unit] =
    for {
      _ <- cluster.shutdown()
    } yield {
      scheduler.shutdown()
    }

  def setAtlas (cohorts: Cohort*) {
    val atlas = Atlas (cohorts.toArray, 1)
    library.atlas = atlas
    library.residents = atlas.residents (localId)
  }

  def issueAtlas (cohorts: Cohort*): Unit =
    librarian.issueAtlas (cohorts.toArray)

  def atlas: Atlas =
    library.atlas

  def unsettled: Boolean =
    !library.atlas.settled

  def deputiesOpen: Boolean =
    !atomic.writers.deputies.isEmpty

  def audit: BatchIterator [(TableId, Cell)] =
    atomic.tstore.tables.entrySet.batch.batchFlatMap { e =>
      e.getValue.iterator (Residents.all) .map { c =>
        (e.getKey, c)
      }}

  def read (rt: TxClock, ops: ReadOp*): Async [Seq [Value]] =
    atomic.read (rt, ops:_*)

  def write (xid: TxId, ct: TxClock, ops: WriteOp*): Async [TxClock] =
    atomic.write (xid, ct, 0, ops:_*)

  def status (xid: TxId): Async [TxStatus] =
    atomic.status (xid)

  def scan (
      table: TableId,
      x: Int,
      start: Bound [Key] = Bound.firstKey,
      window: Window = Window.all,
      slice: Slice = Slice.all,
      batch: Batch = Batch.suggested
  ): CellIterator =
    atomic.scan (table, start, window, slice, batch)

  def putCells (id: TableId, cs: Cell*) (implicit scheduler: StubScheduler): Unit =
    atomic.tstore.receive (id, cs) .expectPass()
}

private object StubAtomicHost extends StoreClusterChecks.Package [StubAtomicHost] {

  def boot (
      id: HostId,
      drive: StubDiskDrive
  ) (implicit
      random: Random,
      parent: Scheduler,
      network: StubNetwork,
      config: StoreTestConfig
  ): Async [StubAtomicHost] = {

    import config._

    implicit val scheduler = new ChildScheduler (parent)
    implicit val cluster = new StubCluster (id)
    implicit val library = new Library
    implicit val recovery = StubDisk.recover()
    implicit val _catalogs = Catalogs.recover()
    val _paxos = Paxos.recover()
    val _atomic = Atomic.recover()

    for {
      launch <- recovery.reattach (drive)
      catalogs <- _catalogs.launch (launch, cluster)
      paxos <- _paxos.launch (launch, cluster)
      atomic <- _atomic.launch (launch, cluster, paxos) .map (_.asInstanceOf [AtomicKit])
    } yield {
      launch.launch()
      new StubAtomicHost (id) (random, scheduler, cluster, drive, launch.disk, library, catalogs, paxos, atomic)
    }}

  def install () (implicit r: Random, s: StubScheduler, n: StubNetwork): Async [StubAtomicHost] = {
    implicit val config = StoreTestConfig()
    boot (r.nextLong, new StubDiskDrive)
  }}
