package com.treode.store.atomic

import scala.util.{Failure, Success}

import com.treode.async.Callback
import com.treode.cluster.RequestDescriptor
import com.treode.store.{ReadOp, TxClock, Value}

private class ReadDeputy (kit: AtomicKit) {
  import kit.{cluster, tables}

  def attach() {
    ReadDeputy.read.listen { case ((rt, ops), from) =>
      tables.read (rt, ops)
    }}}

private object ReadDeputy {

  val read = {
    import AtomicPicklers._
    RequestDescriptor (0xFFC4651219C779C3L, tuple (txClock, seq (readOp)), seq (value))
  }}
