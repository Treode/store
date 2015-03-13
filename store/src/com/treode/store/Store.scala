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

import java.net.SocketAddress
import java.nio.file.Path
import java.util.concurrent.Executors
import scala.util.{Failure, Random}

import com.treode.async.{Async, BatchIterator, Backoff, Scheduler}
import com.treode.async.misc.RichInt
import com.treode.cluster.{CellId, Cluster, ClusterConfig, HostId, Peer, RumorDescriptor}
import com.treode.disk._

import Async.guard

/** The distributed key-value store that's replicated, sharded and transactional.
  *
  */
trait Store {

  /** Read from the database as of a given point in time.
    *
    * Read ensures that the values returned are the most recent value on or before the read time
    * `rt`.  Read also arranges that later writes will be at a time strictly after `rt`.  In other
    * words, if there is serious clock skew across your cluster, future writes will nonetheless
    * receive a timestamp after `rt`.
    *
    * To obtain a consistent view of the database across multiple reads, supply the same value for
    * `rt` to each read.  The maximum timestamp of the values returned from all reads may be used
    * as a condition time on a subsequent write.
    *
    * @param rt The time to read as of, often `TxClock.now`.  Must be within `retention`;
    *   see [[Store.Config]].
    * @param ops The table and key for each row to read.
    * @return The most recent value on or before `rt` and its timestamp for each row; the values
    *   are in the same order as the ops.  The maximum value of the timestamps may be used as a
    *   condition time in a subsequent write.
    * @throws TimeoutException If the reader could not obtain a quorum of replicas for every row.
    *   This may mean hosts are unreachable.
    */
  def read (rt: TxClock, ops: ReadOp*): Async [Seq [Value]]

  /** Write the database conditionally.
    *
    * @param xid The transaction identifier; see [[TxId]] for constraints on on identifiers.
    * @param ct The condition time.  No rows will be written by this operation if any of them have
    *   been written at a timestamp strictly greater than `ct`.
    * @param ops The table, key and operation for each row.  The operation may be to create, hold,
    *   update or delete the row; see [[WriteOp]] for details.
    * @return The timestamp at which the rows have been written.
    * @throws StaleException If any row has been written since `ct`.  If this occurs frequently,
    *   the application may have a hotspot, or there may be significant clock skew across the
    *   cluster.
    * @throws CollisionException If any create tried to overwrite an existing row.
    * @throws TimeoutException If the writer could not obtain a quorum of replicas for every row.
    *   This may happen when hosts are unreachable, or when a distributed deadlock occurred.
    */
  def write (xid: TxId, ct: TxClock, ops: WriteOp*): Async [TxClock]

  /** Check the status of a past transaction.  This is useful when the client looses its connection
    * to the storage cell while awaiting response for a write.  The client may reconnect to any
    * peer in the cell and learn the outcome of its write.  The database maintains a record of all
    * transactions within `retention`; see [[Store.Config]].
    *
    * @param xid The transaction identifier.
    * @return The status.
    * @throws TimeoutException If the system could not obtain a quorum of Paxos acceptors.  This
    *   may mean hosts are unreachable.
    */
  def status (xid: TxId): Async [TxStatus]

  /** Scan the rows of a table.
    *
    * @param table The table to scan.
    * @param start The key at which to begin the scan; use `whilst` of
    *   [[com.treode.async.AsyncIterator AsyncIterator]] to control when it ends.
    * @param window Specify which versions of the row to include in the scan.  A scan can iterate
    *   only the most recent update or all updates over some period.  See [[Window$ Window]] for
    *   details.
    * @param slice A slice of the rows to scan, where that slice respects replica placement; see
    *   [[Slice]] for details.
    * @param batch A hint how to chop the scan into batches; see [[Batch]] for details.
    * @return An [[com.treode.async.AsyncIterator AsyncIterator]] to iterate the rows.  Multiple
    *   updates for a row will be iterated in reverse chronological order.
    * @throws TimeoutException If the scanner could not obtain a quorum of replicas for every
    *   cohort.  This may mean hosts are unreachable.
    */
  def scan (
    table: TableId,
    start: Bound [Key] = Bound.firstKey,
    window: Window = Window.all,
    slice: Slice = Slice.all,
    batch: Batch = Batch.suggested
  ): BatchIterator [Cell]
}

object Store {

  /** This has been moved to package level for easier access in the Scaladoc. */
  @deprecated ("Use StoreConfig", "0.3.0")
  type Config = StoreConfig

  /** This has been moved to package level for easier access in the Scaladoc. */
  @deprecated ("Use StoreConfig", "0.3.0")
  val Config = StoreConfig

  /** This has been moved to package level for easier access in the Scaladoc. */
  @deprecated ("Use StoreController", "0.3.0")
  type Controller = StoreController

  /** This has been moved to package level for easier access in the Scaladoc. */
  @deprecated ("Use StoreRecovery", "0.3.0")
  type Recovery = StoreRecovery

  def init (
      hostId: HostId,
      cellId: CellId,
      superBlockBits: Int,
      segmentBits: Int,
      blockBits: Int,
      diskBytes: Long,
      paths: Path*
  ): Unit = {
    val sysid = SystemId (cellId.id, hostId.id)
    Disk.init (sysid, superBlockBits, segmentBits, blockBits, diskBytes, paths: _*)
  }

  def recover() (implicit
      random: Random,
      scheduler: Scheduler,
      recovery: DiskRecovery,
      config: StoreConfig
  ): StoreRecovery =
    new RecoveryKit

  def recover (
      bindAddr: SocketAddress,
      shareAddr: SocketAddress,
      paths: Path*
  ) (implicit
      diskConfig: DiskConfig,
      clusterConfig: ClusterConfig,
      storeConfig: StoreConfig,
      scheduler: Scheduler
  ): Async [StoreController] = {
    guard {
      implicit val random = Random
      implicit val _disk = Disk.recover()
      val _store = Store.recover()
      for {
        launch <- _disk.reattach (paths: _*)
        cellId = CellId (launch.sysid.id1)
        hostId = HostId (launch.sysid.id2)
        cluster = Cluster.live (cellId, hostId, bindAddr, shareAddr)
        store <- _store.launch (launch, cluster)
      } yield {
        launch.launch()
        cluster.startup()
        (new ExtendedController (launch.controller, cluster, store)): StoreController
      }
    }}}
