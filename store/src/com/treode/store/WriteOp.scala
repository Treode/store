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
        0x1 -> wrap [(TableId, Bytes, Bytes), Create] (
            tuple (tableId, bytes, bytes),
            (v => Create (v._1, v._2, v._3)),
            (v => (v.table, v.key, v.value))),
        0x2 -> wrap [(TableId, Bytes), Hold] (
            tuple (tableId, bytes),
            (v => Hold (v._1, v._2)),
            (v => (v.table, v.key))),
        0x3 -> wrap [(TableId, Bytes, Bytes), Update] (
            tuple (tableId, bytes, bytes),
            (v => Update (v._1, v._2, v._3)),
            (v => (v.table, v.key, v.value))),
        0x4 -> wrap [(TableId, Bytes), Delete] (
            tuple (tableId, bytes),
            (v => Delete (v._1, v._2)),
            (v => (v.table, v.key))))
  }}
