package com.treode.store

case class ReadOp (table: TableId, key: Bytes)

object ReadOp {

  val pickle = {
    import StorePicklers._
    wrap [(TableId, Bytes), ReadOp] (
        tuple (tableId, bytes),
        (v => ReadOp (v._1, v._2)),
        (v => (v.table, v.key)))
  }}
