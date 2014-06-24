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

import java.util.concurrent.{TimeoutException => JTimeoutException}
import java.util.logging.{Level, Logger}

import com.treode.async.AsyncIterator
import com.treode.cluster.{RemoteException => CRemoteException, PortId}

import Level.WARNING

package store {

  class CollisionException (val indexes: Seq [Int]) extends Exception

  class StaleException extends Exception

  class TimeoutException extends JTimeoutException

  class RemoteException extends CRemoteException

  trait Op {
    def table: TableId
    def key: Bytes
  }}

package object store {

  type CellIterator = AsyncIterator [Cell]

  private [store] implicit class RichCellIterator (iter: CellIterator) {

    def dedupe: CellIterator =
      Filters.dedupe (iter)

    def retire (limit: TxClock): CellIterator =
      Filters.retire (iter, limit)

    def slice (table: TableId, slice: Slice): CellIterator =
      iter.filter (c => slice.contains (Cell.locator, (table, c.key)))

    def window (window: Window) =
      window.filter (iter)
  }

  private [store] object log {

    val logger = Logger.getLogger ("com.treode.store")

    def exceptionPreparingWrite (e: Throwable): Unit =
      logger.log (WARNING, s"Exception preparing write", e)

    def catalogUpdateMissingDiffs (id: PortId): Unit =
      logger.log (WARNING, s"A catalog update was missing diffs: $id")
  }}
