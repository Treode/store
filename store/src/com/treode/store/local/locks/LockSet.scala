package com.treode.store.local.locks

import com.treode.store.TxClock

private [local] trait LockSet {

  def ft: TxClock

  /** Releases the acquired locks; only necessary for writes. */
  def release()
}
