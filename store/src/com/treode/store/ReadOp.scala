package com.treode.store

case class ReadOp (table: TableId, key: Bytes)

object ReadOp {

  val pickle = {
    import StorePicklers._
    wrap (tableId, bytes) (apply _) (v => (v.table, v.key))
  }}
