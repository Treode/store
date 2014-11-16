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

package com.treode

import java.net.SocketAddress
import java.util.concurrent.{TimeoutException => JTimeoutException}
import java.util.logging.{Level, Logger}, Level.WARNING

import com.treode.async.{Async, AsyncIterator, BatchIterator}
import com.treode.cluster.{RemoteException => CRemoteException, HostId, PortId}

package store {

  class CollisionException (val indexes: Seq [Int]) extends Exception

  class StaleException extends Exception

  class TimeoutException extends JTimeoutException

  class RemoteException extends CRemoteException

  trait Op {
    def table: TableId
    def key: Bytes
  }

  /** The weighted preference of a peer for scanning a slice.
    *
    * Each peer hosts some shards of data, and it creates less network traffic when scanning the
    * shards it hosts. For a [[Slice]] of data, you can find out which peers host the most shards
    * of that slice, then contact one of those peers to perform the scan. You may perform the scan
    * on any peer, but you will induce less load on the network by choosen a highly weighted peer.
    */
  case class Preference (
    /** The weight of this peer; the number of shards of the slice it holds. */
    weight: Int,

    /** The host ID of this peer. */
    hostId: HostId,

    /** The address this peer advertised for client connections. */
    addr: Option [SocketAddress],

    /** The address this peer advertised for SSL client connections. */
    sslAddr: Option [SocketAddress]
  )
}

package object store {

  type CellIterator = AsyncIterator [Cell]

  type CellIterator2 = BatchIterator [Cell]

  @deprecated ("Use Retention", "0.2.0")
  type PriorValueEpoch = Retention

  @deprecated ("Use Retention", "0.2.0")
  val PriorValueEpoch = Retention

  private [store] implicit class RichCellIterator (iter: CellIterator2) {

    def rebatch (count: Int, bytes: Int): Async [(Seq [Cell], Option [Cell])] = {
      @volatile var _count = count
      @volatile var _bytes = bytes
      iter.toSeqWhile { cell =>
        _bytes -= cell.byteSize
        val r = _count > 0 && (_bytes > 0 || _count == count)
        _count -= 1
        r
      }}

    def dedupe: CellIterator2 =
      iter.filter (Filters.dedupe)

    def retire (limit: TxClock): CellIterator2 =
      iter.filter (Filters.retire (limit))

    def slice (table: TableId, slice: Slice): CellIterator2 =
      iter.filter (c => slice.contains (Cell.locator, (table, c.key)))

    def window (window: Window) =
      iter.filter (window.filter)
  }

  private [store] object log {

    val logger = Logger.getLogger ("com.treode.store")

    def exceptionPreparingWrite (e: Throwable): Unit =
      logger.log (WARNING, s"Exception preparing write", e)

    def catalogUpdateMissingDiffs (id: PortId): Unit =
      logger.log (WARNING, s"A catalog update was missing diffs: $id")
  }}
