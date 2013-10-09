package com.treode.store.lock

import scala.language.postfixOps

import com.treode.cluster.misc.toRunnable
import com.treode.store.TxClock

private [store] class LockSpace (bits: Int) {

  require (bits < 32)

  private val mask = (1 << bits) - 1
  private val locks = Array.fill (1 << bits) (new Lock)

  // Returns true if the lock was granted immediately.  Queues the acquisition to be called back
  // later otherwise.
  private [lock] def read (id: Int, r: Reader): Boolean =
    locks (id) .read (r)

  // Returns the forecasted timestamp, and the writer must commit at a greater timestamp, if the
  // lock was granted immediately.  Queues the acquisition to be called back otherwise.
  private [lock] def write (id: Int, w: Writer): Option [TxClock] =
    locks (id) .write (w)

  private [lock] def release (id: Int, w: Writer): Unit =
    locks (id) .release (w)

  def read (rt: TxClock, ids: Seq [Int]) (cb: => Any): Unit =
    new Reader (rt, toRunnable (cb)) .init (this, ids map (_ & mask) toSet)

  def write (ft: TxClock, ids: Seq [Int]) (cb: LockSet => Any): Unit =
    new Writer (this, ids map (_ & mask) toSet, ft, cb) .init()
}
