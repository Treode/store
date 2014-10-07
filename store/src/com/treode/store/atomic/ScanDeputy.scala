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

import com.treode.async.Async
import com.treode.cluster.RequestDescriptor
import com.treode.store.{Bytes, Bound, Cell, Key, Slice, TableId, TxClock, Window}

import Async.supply
import ScanDeputy._

private class ScanDeputy (kit: AtomicKit) {
  import kit.{cluster, disk, tstore}
  import kit.config.{scanBatchBytes, scanBatchEntries}

  def scan (table: TableId, start: Bound [Key], window: Window, slice: Slice): Async [(Cells, Point)] =
    disk.join {

      val builder = Seq.newBuilder [Cell]
      var entries = 0
      var bytes = 0

      tstore.scan (table, start, window, slice) .whilst { cell =>
        entries < scanBatchEntries &&
        bytes < scanBatchBytes
      } { cell =>
        supply (builder += cell)
      } .map {
        case Some (cell) => (builder.result, Some (cell.timedKey))
        case None => (builder.result, None)
      }}

  def attach() {
    ScanDeputy.scan.listen { case ((table, start, window, slice), from) =>
      scan (table, start, window, slice)
    }}}

private object ScanDeputy {

  type Cells = Seq [Cell]
  type Point = Option [Key]

  val scan = {
    import AtomicPicklers._
    RequestDescriptor (
        0xFF9A8D740D013A6BL,
        tuple (tableId, bound (key), window, slice),
        tuple (seq (cell), option (key)))
  }}
