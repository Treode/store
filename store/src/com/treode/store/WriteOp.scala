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
        0x1 -> wrap3 (tableId, bytes, bytes) (Create.apply _) (v => (v.table, v.key, v.value)),
        0x2 -> wrap2 (tableId, bytes) (Hold.apply _) (v => (v.table, v.key)),
        0x3 -> wrap3 (tableId, bytes, bytes) (Update.apply _) (v => (v.table, v.key, v.value)),
        0x4 -> wrap2 (tableId, bytes) (Delete.apply _) (v => (v.table, v.key)))
  }}
