package com.treode.store

case class WriteBatch (xid: TxId, ct: TxClock, ft: TxClock, ops: Seq [WriteOp]) {
  require (ct <= ft)
}

object WriteBatch {

  def apply (xid: TxId, ct: TxClock, ft: TxClock, op: WriteOp, ops: WriteOp*): WriteBatch =
    WriteBatch (xid, ct, ft, op +: ops)

  val pickle = {
    import StorePicklers._
    wrap [(TxId, TxClock, TxClock, Seq [WriteOp]), WriteBatch] (
        tuple (txId, txClock, txClock, seq (writeOp)),
        (v => WriteBatch (v._1, v._2, v._3, v._4)),
        (v => (v.xid, v.ct, v.ft, v.ops)))
  }}
