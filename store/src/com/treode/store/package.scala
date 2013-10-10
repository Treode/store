package com.treode

import com.treode.cluster.concurrent.Callback
import com.treode.cluster.events.Events

package store {

  case class ReadOp (table: TableId, key: Bytes)

  case class ReadBatch (rt: TxClock, ops: Seq [ReadOp])

  case class Value (time: TxClock, value: Option [Bytes])

  trait ReadCallback extends Callback [Seq [Value]]

  sealed abstract class WriteOp {
    def table: TableId
    def key: Bytes
  }

  object WriteOp {
    case class Create (table: TableId, key: Bytes, value: Bytes) extends WriteOp
    case class Hold (table: TableId, key: Bytes) extends WriteOp
    case class Update (table: TableId, key: Bytes, value: Bytes) extends WriteOp
    case class Delete (table: TableId, key: Bytes) extends WriteOp
  }

  case class WriteBatch (xid: TxId, ct: TxClock, ft: TxClock, ops: Seq [WriteOp]) {
    require (ct <= ft)
  }

  trait Transaction {
    def ft: TxClock
    def commit (wt: TxClock)
    def abort()
  }

  trait WriteCallback extends Callback [Transaction] {
    def advance()
    def conflicts (ks: Set [Int])
  }

  trait Store {
    def read (batch: ReadBatch, cb: ReadCallback)
    def write (batch: WriteBatch, cb: WriteCallback)
  }}

package object store {

  private [store] implicit class StoreEvents (events: Events) {

    def exceptionAbortedRead (e: Throwable): Unit =
      events.warning (s"Aborting read due to exception", e)

    def exceptionAbortedAudit (id: TxId, e: Throwable): Unit =
      events.warning (s"Aborting audit $id due to exception", e)
  }}
