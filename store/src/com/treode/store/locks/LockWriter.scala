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
import com.treode.async.Callback
import com.treode.async.implicits._
import com.treode.store.TxClock

/** Tracks the acquisition of locks and invokes the callback when they have all been granted.
  * @param space The parent LockSpace.
  * @param _ft The forecasted lower bound for the write timestamp.
  * @param ids The locks that are needed.
  * @param cb The callback to invoke when the locks have been acquired.
  */
private class LockWriter (

    space: LockSpace,

    // We raise the forecast as locks are granted.
    private var _ft: TxClock,

    // We null the ids to indicate they were release, and thus prevent double releases.
    private var ids: SortedSet [Int],

    // We null the cb when invoking it to prevent early release and memory leaks.
    private var cb: Callback [LockSet]

) extends LockSet {

  /** For testing mocks. */
  def this() = this (null, TxClock.MinValue, SortedSet.empty, Callback.ignore)

  /** The remaining locks that are needed. */
  private val iter = ids.iterator

  private def finish() {
    val cb = this.cb
    this.cb = null
    cb.pass (this)
  }

  /** Attempt to acquire the locks.  Some of them will be granted immediately, then we will need
    * to wait for one, which will be granted later by a call to [[grant]].  Do this in ascending
    * order of lock id to prevent deadlocks.
    */
  private def acquire(): Boolean = {
    while (iter.hasNext) {
      val id = iter.next
      space.write (id, this) match {
        case Some (ft) if _ft < ft => _ft = ft
        case Some (_) => ()
        case None => return false
      }}
    true
  }

  /** Start aquiring locks in order of id. May obtain them all and invoke the callback promptly,
    * or may acquire only some and wait on one.
    */
  def init() {
    val ready = synchronized {
      acquire()
    }
    if (ready)
      finish()
  }

  /** Continue acquiring locks in order of id. We obtained the one lock we were waiting on. May
    * obtain the rest and invoke the callback, or may acquire only some and wait on another one.
    * @param ft A lower bound for the write timestamp.
    */
  def grant (ft: TxClock): Unit = {
    val ready = synchronized {
      if (_ft < ft)
        _ft = ft
      acquire()
    }
    if (ready)
      finish()
  }

  def ft = _ft

  def release() {
    require (cb == null, "Locks cannot be released until acquired.")
    require (ids != null, "Locks were already released.")
    ids foreach (space.release (_, this))
    ids = null
  }

  override def toString = s"LockWriter (ft=$ft, ready=${!iter.hasNext})"
}
