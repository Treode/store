package com.treode.store

import com.treode.async.AsyncIterator
import com.treode.store.locks.LockSet

package atomic {

  private sealed abstract class PrepareResult

  private object PrepareResult {
    case class Prepared (vt: TxClock, locks: LockSet) extends PrepareResult
    case class Collided (ks: Seq [Int]) extends PrepareResult
    case object Stale extends PrepareResult
  }}

package object atomic {

  private [atomic] type TimedIterator = AsyncIterator [TimedCell]
}
