package com.treode.store.local

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.cluster.events.Events
import com.treode.concurrent.Callback
import com.treode.store._
import com.treode.store.local.locks.LockSet

private class TimedWriter (
    val ct: TxClock,
    ops: Seq [WriteOp],
    store: PreparableStore,
    private var locks: LockSet,
    private var cb: PrepareCallback) extends Transaction {

  private var _awaiting = ops.size
  private var _advance = TxClock.zero
  private var _collisions = Set.empty [Int]
  private var _failures = new ArrayList [Throwable]
  private var _forecast = locks.ft

  private def finish() {
    val cb = this.cb
    this.cb = null
    if (!(_advance == TxClock.zero)) {
      locks.release()
      locks = null
      cb.advance()
    } else if (!_collisions.isEmpty) {
      locks.release()
      locks = null
      cb.collisions (_collisions)
    } else if (!_failures.isEmpty) {
      locks.release()
      locks = null
      cb.fail (MultiException (_failures.toSeq))
    } else {
      cb.apply (this)
    }}

  def ft = _forecast

  def prepare (vt: TxClock) {
    val ready = synchronized {
      if (_forecast < vt) _forecast = vt
      _awaiting -= 1
      _awaiting == 0
    }
    if (ready)
      finish()
  }

  def advance (t: TxClock) {
    val ready = synchronized {
      if (_advance < t) _advance = t
      _awaiting -= 1
      _awaiting == 0
    }
    if (ready)
      finish()
  }

  def conflict (n: Int) {
    val ready = synchronized {
      _collisions += n
      _awaiting -= 1
      _awaiting == 0
    }
    if (ready)
      finish()
  }

  def fail (t: Throwable) {
    val ready = synchronized {
      _failures.add (t)
      _awaiting -= 1
      _awaiting == 0
    }
    if (ready)
      finish()
  }

  def commit (wt: TxClock, cb: Callback [Unit]) {
    val cb1 = new Callback [Unit] {
      def pass (v: Unit) {
        locks.release()
        locks = null
        cb()
      }
      def fail (t: Throwable) = cb.fail (t)
    }
    Callback.guard (cb1) {
      require (this.cb == null, "Transaction cannot be closed until prepared.")
      require (locks != null, "Transaction already closed.")
      store.commit (wt, ops, cb1)
    }}

  def abort() {
    require (this.cb == null, "Transaction cannot be closed until prepared.")
    require (locks != null, "Transaction already closed.")
    locks.release()
    locks = null
  }

  override def toString = f"TimedWriter:${System.identityHashCode(this)}%08X"
}
