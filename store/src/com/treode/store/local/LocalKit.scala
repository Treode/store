package com.treode.store.local

import com.treode.async.{Callback, guard}
import com.treode.store._
import com.treode.store.locks.LockSpace

private abstract class LocalKit (bits: Int) extends LocalStore {

  private val space = new LockSpace (bits)

  def getTimedTable (id: TableId): TimedTable

  def read (rt: TxClock, ops: Seq [ReadOp], cb: ReadCallback): Unit =
    guard (cb) {
      require (!ops.isEmpty, "Read needs at least one operation")
      val ids = ops map (op => (op.table, op.key).hashCode)
      space.read (rt, ids) {
        val r = new TimedReader (rt, ops, cb)
        for ((op, i) <- ops.zipWithIndex)
          getTimedTable (op.table) .read (op.key, i, r)
      }}

  def prepare (ct: TxClock, ops: Seq [WriteOp], cb: PrepareCallback): Unit =
    guard (cb) {
      require (!ops.isEmpty, "Prepare needs at least one operation")
      val ids = ops map (op => (op.table, op.key).hashCode)
      space.write (TxClock.now, ids) { locks =>
        val w = new TimedWriter (ct, ops, this, locks, cb)
        for ((op, i) <- ops.zipWithIndex) {
          import WriteOp._
          val t = getTimedTable (op.table)
          op match {
            case op: Create => t.create (op.key, i, w)
            case op: Hold   => t.prepare (op.key, w)
            case op: Update => t.prepare (op.key, w)
            case op: Delete => t.prepare (op.key, w)
          }}}}

  def commit (wt: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]): Unit =
    guard (cb) {
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
