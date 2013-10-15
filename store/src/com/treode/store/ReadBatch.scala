package com.treode.store

case class ReadBatch (rt: TxClock, ops: Seq [ReadOp])

object ReadBatch {

  def apply (rt: TxClock, op: ReadOp, ops: ReadOp*): ReadBatch =
    ReadBatch (rt, op +: ops)

  val pickle = {
    import StorePicklers._
    wrap (txClock, seq (readOp)) (apply _) (v => (v.rt, v.ops))
  }}
