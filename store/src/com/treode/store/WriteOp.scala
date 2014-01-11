package com.treode.store

sealed abstract class WriteOp {
  def table: TableId
  def key: Bytes
}

object WriteOp {

  case class Create (table: TableId, key: Bytes, value: Bytes) extends WriteOp

  case class Hold (table: TableId, key: Bytes) extends WriteOp

  case class Update (table: TableId, key: Bytes, value: Bytes) extends WriteOp

  case class Delete (table: TableId, key: Bytes) extends WriteOp

  val pickle = {
    import StorePicklers._
    tagged [WriteOp] (
        0x1 -> wrap (tableId, bytes, bytes)
            .build ((Create.apply _).tupled)
            .inspect (v => (v.table, v.key, v.value)),
        0x2 -> wrap (tableId, bytes)
            .build ((Hold.apply _).tupled)
            .inspect (v => (v.table, v.key)),
        0x3 -> wrap (tableId, bytes, bytes)
            .build ((Update.apply _).tupled)
            .inspect (v => (v.table, v.key, v.value)),
        0x4 -> wrap (tableId, bytes)
            .build ((Delete.apply _).tupled)
            .inspect (v => (v.table, v.key)))
  }}
