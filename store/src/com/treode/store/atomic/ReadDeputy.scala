package com.treode.store.atomic

import com.treode.async.Callback
import com.treode.cluster.RequestDescriptor
import com.treode.store.{ReadOp, TxClock, Value}

private class ReadDeputy (kit: AtomicKit) {
  import kit.{cluster, store}

  type ReadMediator = ReadDeputy.read.Mediator

  def read (mdtr: ReadMediator, rt: TxClock, ops: Seq [ReadOp]) {
    store.read (rt, ops) run (new Callback [Seq [Value]] {

      def pass (vs: Seq [Value]): Unit =
        mdtr.respond (ReadResponse.Got (vs))

      def fail (t: Throwable): Unit =
        mdtr.respond (ReadResponse.Failed)
    })
  }

  def attach() {
    ReadDeputy.read.listen { case ((rt, ops), mdtr) =>
      read (mdtr, rt, ops)
    }}}

private object ReadDeputy {

  val read = {
    import AtomicPicklers._
    RequestDescriptor (0xFFC4651219C779C3L, tuple (txClock, seq (readOp)), readResponse)
  }}
