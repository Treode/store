package com.treode.store.local.locks

import scala.collection.SortedSet

import com.treode.store.TxClock

// Tracks the acquisition of locks and invokes the callback when they have all been granted.
private class Writer (
    space: LockSpace,
    ids: SortedSet [Int],
    _ft: TxClock,
    private var cb: LockSet => Any) extends LockSet {

  // For testing mocks.
  def this() = this (null, SortedSet.empty, TxClock.zero, _ => ())

  private var iter = ids.iterator
  private var max = _ft

  private def finish() {
    val cb = this.cb
    this.cb = null
    cb (this)
  }

  // Attempt to acquire the locks.  Some of them will be granted immediately, then we will need
  // to wait for one, which will be granted later by a call to grant.  Do this in ascending order
  // of lock id to prevent deadlocks.
  private def acquire(): Boolean = {
    while (iter.hasNext) {
      val id = iter.next
      space.write (id, this) match {
        case Some (max) =>
          if (this.max < max)
            this.max = max
        case None =>
          return false
      }}
    true
  }

  def init() {
    val ready = synchronized {
      acquire()
    }
    if (ready)
      finish()
  }

  def grant (max: TxClock): Unit = {
    val ready = synchronized {
      if (this.max < max)
        this.max = max
      acquire()
    }
    if (ready)
      finish()
  }

  def ft = max

  def release() {
    require (cb == null, "Locks cannot be released.")
    ids foreach (space.release (_, this))
  }}
