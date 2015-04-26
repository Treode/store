/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.store.locks

import scala.collection.SortedSet
import scala.language.postfixOps

import com.treode.async.{Async, Scheduler}
import com.treode.store.{StoreConfig, TxClock}

import Async.async

private [store] class LockSpace (implicit config: StoreConfig) {
  import config.lockSpaceBits

  /** A mask to quickly hash a lock ID onto the lock space by using bitwise-and. */
  private val mask = (1 << lockSpaceBits) - 1

  /** The set of all lock IDs, for scan; saves allocation and initialization time. */
  private val all = (0 to mask) .toSet

  /** The locks. */
  private val locks = Array.fill (1 << lockSpaceBits) (new Lock)

  /** Returns true if the lock was granted immediately.  Queues the acquisition to be called back
    * later otherwise.
    */
  private [locks] def read (id: Int, r: LockReader): Boolean =
    locks (id) .read (r)

  /** Returns the forecasted timestamp, and the writer must commit at a greater timestamp, if the
    * lock was granted immediately.  Queues the acquisition to be called back otherwise.
    */
  private [locks] def write (id: Int, w: LockWriter): Option [TxClock] =
    locks (id) .write (w)

  /** Release the lock, which must be held by the given writer. */
  private [locks] def release (id: Int, w: LockWriter): Unit =
    locks (id) .release (w)

  /** Acquire the locks for read. Returns when the locks are acquired. Read locks do not need to
    * be released.
    * @param rt The timestamp to read as-of.
    * @param ids The locks to acquire.
    */
  def read (rt: TxClock, ids: Seq [Int]): Async [Unit] =
    async { cb =>
      val _ids = ids map (_ & mask) toSet;
      new LockReader (rt, cb) .init (this, _ids)
    }

  /** Acquire the locks for write. Returns when the locks are acquired. The writer must respect
    * the lower bound for the write timestamp, and must release the locks.
    * @param ft A forecasted lower bound for the write timestamp.
    * @param ids The locks to acquire.
    * @return A LockSet with a lower bound for the write timestamp, and a method to release the
    * locks.
    */
  def write (ft: TxClock, ids: Seq [Int]): Async [LockSet] =
    async { cb =>
      val _ids = SortedSet (ids map (_ & mask): _*)
      new LockWriter (this, ft, _ids, cb) .init()
    }

  /** Acquire the locks for scan. Returns when the all locks are acquired. Read locks do not need
    * to be released.
    * @param rt The timestamp to read as-of.
    */
  def scan (rt: TxClock): Async [Unit] =
    async { cb =>
      new LockReader (rt, cb) .init (this, all)
    }}
