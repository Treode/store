package com.treode.store

/** The table and key of a row to read. */
case class ReadOp (table: TableId, key: Bytes) extends Op

object ReadOp {

  val pickler = {
    import StorePicklers._
    wrap (tableId, bytes)
    .build ((apply _).tupled)
    .inspect (v => (v.table, v.key))
  }}
