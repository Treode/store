package com.treode.store.locks

import com.treode.store.TxClock

private [store] trait LockSet {

  def ft: TxClock

  /** Releases the acquired locks; only necessary for writes. */
  def release()
}
