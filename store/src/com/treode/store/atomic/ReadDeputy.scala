package com.treode.store.atomic

import scala.util.{Failure, Success}

import com.treode.async.Callback
import com.treode.cluster.RequestDescriptor
import com.treode.store.{ReadOp, TxClock, Value}

private class ReadDeputy (kit: AtomicKit) {
  import kit.{cluster, tables}

  type ReadMediator = ReadDeputy.read.Mediator

  def read (mdtr: ReadMediator, rt: TxClock, ops: Seq [ReadOp]) {
    tables.read (rt, ops) run {
      case Success (vs) =>
        mdtr.respond (ReadResponse.Got (vs))
      case Failure (t) =>
        mdtr.respond (ReadResponse.Failed)
    }}

  def attach() {
    ReadDeputy.read.listen { case ((rt, ops), mdtr) =>
      read (mdtr, rt, ops)
    }}}

private object ReadDeputy {

  val read = {
    import AtomicPicklers._
    RequestDescriptor (0xFFC4651219C779C3L, tuple (txClock, seq (readOp)), readResponse)
  }}
