package com.treode.store

case class WriteBatch (xid: TxId, ct: TxClock, ft: TxClock, ops: Seq [WriteOp]) {
  require (ct <= ft)
}

object WriteBatch {

  def apply (xid: TxId, ct: TxClock, ft: TxClock, op: WriteOp, ops: WriteOp*): WriteBatch =
    WriteBatch (xid, ct, ft, op +: ops)

  val pickle = {
    import StorePicklers._
    wrap (txId, txClock, txClock, seq (writeOp)) (apply _) (v => (v.xid, v.ct, v.ft, v.ops))
  }}
