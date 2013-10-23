package com.treode.store.local

import com.treode.cluster.events.Events
import com.treode.concurrent.Callback
import com.treode.store._
import com.treode.store.lock.{LockSet, LockSpace}

abstract class LocalStore extends Store {

  protected def space: LockSpace

  protected def table (id: TableId): Table

  def read (batch: ReadBatch, cb: ReadCallback): Unit =
    Callback.guard (cb) {
      require (!batch.ops.isEmpty, "Batch must include at least one operation")
      val ids = batch.ops map (op => (op.table, op.key).hashCode)
      space.read (batch.rt, ids) {
        val r = new Reader (batch, cb)
        for ((op, i) <- batch.ops.zipWithIndex)
          table (op.table) .read (op.key, i, r)
      }}

  def write (batch: WriteBatch, cb: WriteCallback): Unit =
    Callback.guard (cb) {
      require (!batch.ops.isEmpty, "Batch must include at least one operation")
      val ids = batch.ops map (op => (op.table, op.key).hashCode)
      space.write (batch.ft, ids) { locks =>
        val w = new Writer (batch, locks, cb)
        for ((op, i) <- batch.ops.zipWithIndex) {
          import WriteOp._
          val t = table (op.table)
          op match {
            case op: Create => t.create (op.key, op.value, i, w)
            case op: Hold   => t.hold (op.key, w)
            case op: Update => t.update (op.key, op.value, w)
            case op: Delete => t.delete (op.key, w)
          }}}}
}
