package com.treode.store

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.async.{Callback, MultiException}
import com.treode.store._
import com.treode.store.locks.LockSet

private class TimedWriter (
    val ct: TxClock,
    private var count: Int,
    private val locks: LockSet,
    private val cb: PrepareCallback) {

  private var _version = TxClock.zero
  private var _collisions = Set.empty [Int]
  private var _failures = new ArrayList [Throwable]

  private def release() {
    require (count > 0, "TimedWriter was already released.")
    count -= 1
    if (count > 0) return
    if (ct < _version) {
      locks.release()
      cb.advance()
    } else if (!_collisions.isEmpty) {
      locks.release()
      cb.collisions (_collisions)
    } else if (!_failures.isEmpty) {
      locks.release()
      cb.fail (MultiException.fit (_failures.toSeq))
    } else {
      cb.pass (new Preparation (ct, _version, locks))
    }}

  def advance (t: TxClock): Unit = synchronized {
    if (_version < t) _version = t
    release()
  }

  def conflict (n: Int): Unit = synchronized {
    _collisions += n
    release()
  }

  def fail (t: Throwable): Unit = synchronized {
    _failures.add (t)
    release()
  }

  override def toString = f"TimedWriter:${System.identityHashCode(this)}%08X"
}
