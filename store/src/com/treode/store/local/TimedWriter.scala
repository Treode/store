package com.treode.store.local

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.cluster.events.Events
import com.treode.concurrent.Callback
import com.treode.store._
import com.treode.store.local.locks.LockSet

private class TimedWriter (
    batch: WriteBatch,
    locks: LockSet,
    private var cb: WriteCallback) extends Transaction {

  private var _ops = new ArrayList [TxClock => Any]
  private var _awaiting = batch.ops.size
  private var _advance = TxClock.zero
  private var _collisions = Set.empty [Int]
  private var _failures = new ArrayList [Throwable]

  private def finish() {
    val cb = this.cb
    this.cb = null
    if (!(_advance == TxClock.zero)) {
      locks.release()
      cb.advance()
    } else if (!_collisions.isEmpty) {
      locks.release()
      cb.collisions (_collisions)
    } else if (!_failures.isEmpty) {
      locks.release()
      cb.fail (MultiException (_failures.toSeq))
    } else {
      cb.apply (this)
    }}

  def ct = batch.ct

  def ft = locks.ft

  def prepare (f: TxClock => Any) {
    val ready = synchronized {
      _ops.add (f)
      _awaiting -= 1
      _awaiting == 0
    }
    if (ready)
      finish()
  }

  def prepare() {
    val ready = synchronized {
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

  def commit (wt: TxClock) {
    require (cb == null, "Transaction cannot be committed.")
    require (wt > ft)
    _ops foreach (_ (wt))
    locks.release()
  }

  def abort() {
    require (cb == null, "Transaction cannot be aborted.")
    locks.release()
  }}
