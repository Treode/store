package com.treode.store.lock

import com.treode.cluster.misc.toRunnable
import com.treode.store.TxClock

// Tracks the acquisition of locks and invokes the callback when they have all been granted.
private class Reader (_rt: TxClock, cb: Runnable) {

  // For testing mocks.
  def this() = this (TxClock.Zero, toRunnable(()))

  private var needed = 0

  // Attempt to acquire the locks.  Some of them will be granted immediately.  For others, we
  // will receive a callback via grant().
  def init (space: LockSpace, ids: Set [Int]) {
    val ready = synchronized {
      for (id <- ids)
        if (!space.read (id, this))
          needed += 1
      needed == 0
    }
    if (ready)
      cb.run()
  }

  def rt = _rt

  def grant(): Unit = {
    val ready = synchronized {
      needed -= 1
      needed == 0
    }
    if (ready)
      cb.run()
  }}
