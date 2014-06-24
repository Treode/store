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

import com.treode.async.{Callback, Scheduler}
import com.treode.async.implicits._
import com.treode.store.TxClock

// Tracks the acquisition of locks and invokes the callback when they have all been granted.
private class LockReader (_rt: TxClock, cb: Callback [Unit]) {

  // For testing mocks.
  def this() = this (TxClock.MinValue, Callback.ignore)

  private var needed = 0

  private def finish(): Unit =
    cb.pass()

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
      finish()
  }

  def rt = _rt

  def grant(): Unit = {
    val ready = synchronized {
      needed -= 1
      needed == 0
    }
    if (ready)
      finish()
  }

  override def toString = s"LockReader (rt=$rt, ready=${needed == 0})"
}
