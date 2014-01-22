package com.treode.store.atomic

import com.treode.cluster.RequestDescriptor
import com.treode.store.{ReadCallback, ReadOp, TxClock, Value}

private class ReadDeputy (kit: AtomicKit) {
  import kit.store

  type ReadMediator = ReadDeputy.read.Mediator

  def read (mdtr: ReadMediator, rt: TxClock, ops: Seq [ReadOp]) {
    store.read (rt, ops, new ReadCallback {
      def pass (vs: Seq [Value]) = mdtr.respond (ReadResponse.Got (vs))
      def fail (t: Throwable) = mdtr.respond (ReadResponse.Failed)
    })
  }}

private object ReadDeputy {

  val read = {
    import AtomicPicklers._
    new RequestDescriptor (0xFFC4651219C779C3L, tuple (txClock, seq (readOp)), readResponse)
  }}
