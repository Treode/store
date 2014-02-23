package com.treode.store

import com.treode.store.locks.LockSet

private class Preparation (ct: TxClock, vt: TxClock, val locks: LockSet) {

  val ft: TxClock =
    TxClock.max (locks.ft, TxClock.max (ct, vt))

  def release(): Unit =
    locks.release()
}
