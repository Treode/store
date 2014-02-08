package com.treode.store

case class ReadOp (table: TableId, key: Bytes)

object ReadOp {

  val pickler = {
    import StorePicklers._
    wrap (tableId, bytes)
    .build ((apply _).tupled)
    .inspect (v => (v.table, v.key))
  }}
