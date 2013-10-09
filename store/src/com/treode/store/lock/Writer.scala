package com.treode.store.lock

import com.treode.store.TxClock

// Tracks the acquisition of locks and invokes the callback when they have all been granted.
private class Writer (
    space: LockSpace,
    ids: Set [Int],
    _ft: TxClock,
    private var cb: LockSet => Any) extends LockSet {

  // For testing mocks.
  def this() = this (null, Set.empty, TxClock.Zero, _ => ())

  private var needed = 0
  private var max = _ft

  private def finish() {
    val cb = this.cb
    this.cb = null
    cb (this)
  }

  // Attempt to acquire the locks.  Some of them will be granted immediately.  For others, we
  // will receive a callback via grant().
  def init() {
    val ready = synchronized {
      for (id <- ids)
        space.write (id, this) match {
          case Some (max) =>
            if (this.max < max) this.max = max
          case None =>
            needed += 1
        }
      needed == 0
    }
    if (ready)
      finish()
  }

  def grant (max: TxClock): Unit = {
    val ready = synchronized {
      if (this.max < max) this.max = max
      needed -= 1
      needed == 0
    }
    if (ready)
      finish()
  }

  def ft = max

  def release() {
    require (cb == null, "Locks cannot be released.")
    ids foreach (space.release (_, this))
  }}
