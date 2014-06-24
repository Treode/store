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
import com.treode.store.{Store, TxClock}

import Async.async

private [store] class LockSpace (implicit config: Store.Config) {
  import config.lockSpaceBits

  private val mask = (1 << lockSpaceBits) - 1
  private val all = (0 to mask) .toSet
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
    async { cb =>
      val _ids = ids map (_ & mask) toSet;
      new LockReader (rt, cb) .init (this, _ids)
    }

  def write (ft: TxClock, ids: Seq [Int]): Async [LockSet] =
    async { cb =>
      val _ids = SortedSet (ids map (_ & mask): _*)
      new LockWriter (this, ft, _ids, cb) .init()
    }

  def scan (rt: TxClock): Async [Unit] =
    async { cb =>
      new LockReader (rt, cb) .init (this, all)
    }}
