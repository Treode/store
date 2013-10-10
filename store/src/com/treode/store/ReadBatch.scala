package com.treode.store

case class ReadBatch (rt: TxClock, ops: Seq [ReadOp])

object ReadBatch {

  def apply (rt: TxClock, op: ReadOp, ops: ReadOp*): ReadBatch =
    ReadBatch (rt, op +: ops)

  val pickle = {
    import StorePicklers._
    wrap [(TxClock, Seq [ReadOp]), ReadBatch] (
        tuple (txClock, seq (readOp)),
        (v => ReadBatch (v._1, v._2)),
        (v => (v.rt, v.ops)))
  }
}
