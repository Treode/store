package com.treode.store.atomic

import scala.util.{Failure, Success}

import com.treode.async.Callback
import com.treode.cluster.{IgnoreRequestException, RequestDescriptor}
import com.treode.store.{ReadOp, TxClock, Value}

private class ReadDeputy (kit: AtomicKit) {
  import kit.{cluster, tables}
  import kit.library.atlas

  def attach() {
    ReadDeputy.read.listen { case ((version, rt, ops), from) =>
      if (atlas.version - 1 <= version && version <= atlas.version + 1)
        tables.read (rt, ops)
      else
        throw new IgnoreRequestException
    }}}

private object ReadDeputy {

  val read = {
    import AtomicPicklers._
    RequestDescriptor (0xFFC4651219C779C3L, tuple (uint, txClock, seq (readOp)), seq (value))
  }}
