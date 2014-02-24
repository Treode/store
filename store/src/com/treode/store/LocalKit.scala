package com.treode.store

import com.treode.async.{Async, Callback}
import com.treode.store._
import com.treode.store.locks.{LockSpace, LockSet}

import Async.async

private abstract class LocalKit (implicit config: StoreConfig) extends LocalStore {

  private val space = new LockSpace

  def getTimedTable (id: TableId): TimedTable

  private def read (rt: TxClock, ids: Seq [Int]) (cb: => Any): Unit =
    space.read (rt, ids) run (new Callback [Unit] {
      def pass (v: Unit) = cb
      def fail (t: Throwable) = throw t
    })

  private def write (rt: TxClock, ids: Seq [Int]) (cb: LockSet => Any): Unit =
    space.write (rt, ids) run (new Callback [LockSet] {
      def pass (v: LockSet) = cb (v)
      def fail (t: Throwable) = throw t
    })

  private def read (rt: TxClock, ops: Seq [ReadOp], cb: ReadCallback): Unit =
    cb.defer {
      require (!ops.isEmpty, "Read needs at least one operation")
      val ids = ops map (op => (op.table, op.key).hashCode)
      read (rt, ids) {
        val r = new TimedReader (rt, ops, cb)
        for ((op, i) <- ops.zipWithIndex)
          getTimedTable (op.table) .read (op.key, i, r)
      }}

  def read (rt: TxClock, ops: Seq [ReadOp]): Async [Seq [Value]] =
    async { cb =>
      read (rt, ops, new ReadCallback {
        def pass (vs: Seq [Value]) = cb.pass (vs)
        def fail (t: Throwable) = cb.fail (t)
      })
    }

  private def prepare (ct: TxClock, ops: Seq [WriteOp], cb: PrepareCallback): Unit =
    cb.defer {
      require (!ops.isEmpty, "Prepare needs at least one operation")
      val ids = ops map (op => (op.table, op.key).hashCode)
      write (TxClock.now, ids) { locks =>
        val w = new TimedWriter (ct, ops.size, locks, cb)
        for ((op, i) <- ops.zipWithIndex) {
          import WriteOp._
          val t = getTimedTable (op.table)
          op match {
            case op: Create => t.create (op.key, i, w)
            case op: Hold   => t.prepare (op.key, w)
            case op: Update => t.prepare (op.key, w)
            case op: Delete => t.prepare (op.key, w)
          }}}}

  def prepare (ct: TxClock, ops: Seq [WriteOp]): Async [PrepareResult] =
    async { cb =>
      import PrepareResult._
      prepare (ct, ops, new PrepareCallback {
        def pass (prep: Preparation) = cb.pass (Prepared (prep.ft, prep.locks))
        def collisions (ks: Set [Int]) = cb.pass (Collided (ks.toSeq.sorted))
        def advance() = cb.pass (Stale)
        def fail (t: Throwable) = cb.fail (t)
      })
    }

  private def commit (wt: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]): Unit =
    cb.defer {
      require (!ops.isEmpty, "Commit needs at least one operation")
      val c = new TimedCommitter (ops, cb)
      for (op <- ops) {
        import WriteOp._
        val t = getTimedTable (op.table)
        op match {
          case op: Create => t.create (op.key, op.value, wt, c)
          case op: Hold   => c.pass()
          case op: Update => t.update (op.key, op.value, wt, c)
          case op: Delete => t.delete (op.key, wt, c)
        }}}

  def commit (wt: TxClock, ops: Seq [WriteOp]): Async [Unit] =
    async (commit (wt, ops, _))
}
