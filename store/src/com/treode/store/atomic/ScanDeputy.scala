package com.treode.store.atomic

import scala.util.{Failure, Success}

import com.treode.async.Async
import com.treode.cluster.RequestDescriptor
import com.treode.store.{Bytes, Cell, Key, TableId, TxClock}

import Async.supply
import ScanDeputy._

private class ScanDeputy (kit: AtomicKit) {
  import kit.{cluster, disks, tables}
  import kit.config.{scanBatchBytes, scanBatchEntries}

  def scan (table: TableId, key: Bytes, time: TxClock): Async [(Cells, Point)] =
    disks.join {

      val builder = Seq.newBuilder [Cell]
      var entries = 0
      var bytes = 0

      tables .scan (table, key, time) .whilst { cell =>
        entries < scanBatchEntries &&
        bytes < scanBatchBytes
      } { cell =>
        supply (builder += cell)
      } .map {
        case Some (cell) => (builder.result, Some (cell.timedKey))
        case None => (builder.result, None)
      }}

  def attach() {
    ScanDeputy.scan.listen { case ((table, key, time), from) =>
      scan (table, key, time)
    }}}

private object ScanDeputy {

  type Cells = Seq [Cell]
  type Point = Option [Key]

  val scan = {
    import AtomicPicklers._
    RequestDescriptor (
        0xFF9A8D740D013A6BL,
        tuple (tableId, bytes, txClock),
        tuple (seq (cell), option (key)))
  }}
