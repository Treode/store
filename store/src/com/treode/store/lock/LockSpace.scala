package com.treode.store.lock

import com.treode.cluster.misc.toRunnable
import com.treode.store.{Bytes, TxClock}

private class LockSpace (bits: Int) {

  require (bits < 32)

  private val mask = (1 << bits) - 1
  private val locks = Array.fill (1 << bits) (new Lock)

  // Returns true if the lock was granted immediately.  Queues the acquisition to be called back
  // later otherwise.
  private def read (id: Int, r: ReadAcquisition): Boolean =
    locks (id) .read (r)

  // Returns the forecasted timestamp, and the writer must commit at a greater timestamp, if the
  // lock was granted immediately.  Queues the acquisition to be called back otherwise.
  private def write (id: Int, w: WriteAcquisition): Option [TxClock] =
    locks (id) .write (w)

  // Tracks the acquisition of locks and invokes the callback when they have all been granted.
  private class Reader (val rt: TxClock, ids: Set [Int], cb: Runnable) extends ReadAcquisition {

    private var needed = 0

    // Attempt to acquire the locks.  Some of them will be granted immediately.  For others, we
    // will receive a callback via grant().
    synchronized {
      for (id <- ids)
        if (!read (id, this))
          needed += 1
      if (needed == 0)
        cb.run()
    }

    def grant(): Unit = {
      val ready = synchronized {
        needed -= 1
        needed == 0
      }
      if (ready)
        cb.run()
    }}

  // Tracks the acquisition of locks and invokes the callback when they have all been granted.
  private class Writer (val ft: TxClock, ids: Set [Int], cb: TxClock => Any) extends WriteAcquisition {

    private var needed = 0
    private var max = TxClock.Zero

    // Attempt to acquire the locks.  Some of them will be granted immediately.  For others, we
    // will receive a callback via grant().
    synchronized {
      for (id <- ids)
        write (id, this) match {
          case Some (max) =>
            if (this.max < max) this.max = max
          case None =>
            needed += 1
        }
      if (needed == 0)
        cb (max)
    }

    def grant (max: TxClock): Unit = {
      val ready = synchronized {
        if (this.max < max) this.max = max
        needed -= 1
        needed == 0
      }
      if (ready)
        cb (max)
    }}

  private class Locks (ids: Set [Int]) extends LockSet {

    def read (rt: TxClock) (cb: => Any): Unit =
      new Reader (rt, ids, toRunnable (cb))

    def write (ft: TxClock) (cb: TxClock => Any): Unit =
      new Writer (ft, ids, cb)

    def release(): Unit =
      ids foreach (locks (_) .release())
  }

  def acquire (keys: Seq [Bytes]): LockSet =
    new Locks (keys .map (_.hashCode & mask) .toSet)
}
