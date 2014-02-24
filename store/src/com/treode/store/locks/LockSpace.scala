package com.treode.store.locks

import scala.collection.SortedSet
import scala.language.postfixOps

import com.treode.async.{Async, Scheduler}
import com.treode.store.{StoreConfig, TxClock}

import Async.async

private [store] class LockSpace (implicit config: StoreConfig) {
  import config.lockSpaceBits

  private val mask = (1 << lockSpaceBits) - 1
  private val locks = Array.fill (1 << lockSpaceBits) (new Lock)

  // Returns true if the lock was granted immediately.  Queues the acquisition to be called back
  // later otherwise.
  private [locks] def read (id: Int, r: LockReader): Boolean =
    locks (id) .read (r)

  // Returns the forecasted timestamp, and the writer must commit at a greater timestamp, if the
  // lock was granted immediately.  Queues the acquisition to be called back otherwise.
  private [locks] def write (id: Int, w: LockWriter): Option [TxClock] =
    locks (id) .write (w)

  private [locks] def release (id: Int, w: LockWriter): Unit =
    locks (id) .release (w)

  def read (rt: TxClock, ids: Seq [Int]): Async [Unit] =
    async {cb =>
      val _ids = ids map (_ & mask) toSet;
      new LockReader (rt, cb) .init (this, _ids)
    }

  def write (ft: TxClock, ids: Seq [Int]): Async [LockSet] =
    async { cb =>
      val _ids = SortedSet (ids map (_ & mask): _*)
      new LockWriter (this, ft, _ids, cb) .init()
    }}
