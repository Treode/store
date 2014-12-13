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

import scala.util.{Failure, Success}

import com.treode.async.Async, Async.supply
import com.treode.cluster.RequestDescriptor
import com.treode.store._

import ScanDeputy.Cells

private class ScanDeputy (kit: AtomicKit) {
  import kit.{cluster, disk, tstore}

  def scan (
      table: TableId,
      start: Bound [Key],
      window: Window,
      slice: Slice,
      batch: Batch
  ): Async [(Cells, Option [Cell])] =
    disk.join {
      tstore
      .scan (table, start, window, slice)
      .rebatch (batch)
    }

  def attach() {

    ScanDeputy.scanV0.listen { case ((table, start, window, slice), from) =>
      scan (table, start, window, slice, Batch.suggested)
      .map {case (cells, last) => (cells, last map (_.timedKey))}
    }

    ScanDeputy.scan.listen { case ((table, start, window, slice, batch), from) =>
      scan (table, start, window, slice, batch)
      .map {case (cells, last) => (cells, last.isEmpty)}
    }}}

private object ScanDeputy {

  type Cells = Seq [Cell]

  // TODO: Remove after release of 0.2.0.
  // This remains to allow rolling upgrade of a cluster from 0.1.0 to 0.2.0.
  val scanV0 = {
    import AtomicPicklers._
    RequestDescriptor (
        0xFF9A8D740D013A6BL,
        tuple (tableId, bound (key), window, slice),
        tuple (seq (cell), option (key)))
  }

  val scan = {
    import AtomicPicklers._
    RequestDescriptor (
        0x1A85D54A2346425BL,
        tuple (tableId, bound (key), window, slice, batch),
        tuple (seq (cell), boolean))
  }}
