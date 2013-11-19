package com.treode.store.local

import com.treode.concurrent.Callback
import com.treode.store._
import com.treode.store.local.locks.LockSpace

private abstract class LocalKit (bits: Int) extends LocalStore {

  private val space = new LockSpace (bits)

  def getTimedTable (id: TableId): TimedTable

  def read (batch: ReadBatch, cb: ReadCallback): Unit =
    Callback.guard (cb) {
      require (!batch.ops.isEmpty, "Batch must include at least one operation")
      val ids = batch.ops map (op => (op.table, op.key).hashCode)
      space.read (batch.rt, ids) {
        val r = new TimedReader (batch, cb)
        for ((op, i) <- batch.ops.zipWithIndex)
          getTimedTable (op.table) .read (op.key, i, r)
      }}

  def prepare (ct: TxClock, ops: Seq [WriteOp], cb: PrepareCallback): Unit =
    Callback.guard (cb) {
      require (!ops.isEmpty, "Prepare needs at least one operation")
      val ids = ops map (op => (op.table, op.key).hashCode)
      space.write (ct, ids) { locks =>
        val w = new TimedWriter (ct, ops, this, locks, cb)
        for ((op, i) <- ops.zipWithIndex) {
          import WriteOp._
          val t = getTimedTable (op.table)
          op match {
            case op: Create => t.create (op.key, op.value, i, w)
            case op: Hold   => t.hold (op.key, w)
            case op: Update => t.update (op.key, op.value, w)
            case op: Delete => t.delete (op.key, w)
          }}}}

  def commit (wt: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]): Unit =
    Callback.guard (cb) {
      require (!ops.isEmpty, "Commit needs at least one operation")
      val c = new TimedCommitter (ops, cb)
      for (op <- ops) {
        import WriteOp._
        val t = getTimedTable (op.table)
        op match {
          case op: Create => t.create (op.key, op.value, wt, c)
          case op: Hold   => c()
          case op: Update => t.update (op.key, op.value, wt, c)
          case op: Delete => t.delete (op.key, wt, c)
        }}}
}
