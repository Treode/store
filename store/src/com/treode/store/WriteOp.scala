package com.treode.store

/** The table and key of a row to write, and the update to perform.  The case classes are nested in
  * the [[WriteOp$ companion object]].
  */
sealed abstract class WriteOp extends Op

object WriteOp {

  /** Create the row.  This will succeed if the most recent write is on or before the condition 
    * time (just like update), ''and'' if the most recent write is a delete (unlike update).
    */
  case class Create (table: TableId, key: Bytes, value: Bytes) extends WriteOp

  /** Hold the row.  Use this when you need to ensure that row has not changed since the condition
    * time but do not need to change it as part of this write.
    */
  case class Hold (table: TableId, key: Bytes) extends WriteOp

  /** Update the row.  Set it to a new value if it has not changed since the condition time.
    */
  case class Update (table: TableId, key: Bytes, value: Bytes) extends WriteOp

  /** Delete the row.  Set the row's value to `None` if it has not changed since the condition
    * time.
    */
  case class Delete (table: TableId, key: Bytes) extends WriteOp

  val pickler = {
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
