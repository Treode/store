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

import java.util
import scala.collection.JavaConversions._

import com.treode.store.TxClock

/** A reader/writer lock that implements a Lamport clock.  It allows one writer at a time, and it
  * tracks the lower bound timestamp, which a writer must commit after. It allows multiple readers
  * at a time. When a writer holds the lock, readers using an as-of timestamp strictly after the
  * lower bound are held, whereas readers using an as-of timestamp at or before the lower bound are
  * not held.
  */
private class Lock {

  /** A constant; saves allocation and initialization time. */
  private val NoReaders = new util.ArrayList [LockReader] (0)

  /** The forecasted lower bound timestamp.  All future writers shall commit at a timestamp
    * strictly after this.  Any reader as-of a timestamp at or before this may proceed immediately,
    * since no future writer will invalidate its read.  A reader as-of a timestamp after this must
    * wait for the current writer to release the lock, since that writer could commit the value with
    * timestamp forecast+1.
    */
  private var forecast = TxClock.MinValue

  /** If non-null, a writer holds the lock. */
  private var engaged: LockWriter = null

  /** Readers waiting for the writer to release the lock. */
  private var readers = new util.ArrayList [LockReader]

  /** Writers waiting for the current writer to release the lock. */
  private val writers = new util.ArrayDeque [LockWriter]

  /** A reader wants to acquire the lock to read as-of timestamp `rt`; the lock will ensure no
    * future writer commits at or before that timestamp.
    *
    * If the reader's timestamp `rt` is
    *
    * - less than or equal to the forecasted one, the reader may proceed.
    *
    * - greater than the forecasted one and the lock is free, then the reader may proceed; the lock
    *   raises forecast to ensure no writer commits before or at `rt`.
    *
    * - greater than the forecasted one and a writer holds the lock, then the reader must wait for
    *   the writer to release the lock, since that writer could commit at forecast+1.
    *
    * If the reader may proceed immediately, this returns true.  Otherwise, it returns false and
    * queues the reader to be called back later.
    */
  def read (r: LockReader): Boolean = synchronized {
    if (r.rt <= forecast) {
      true
    } else if (engaged == null) {
      forecast = r.rt
      true
    } else {
      readers.add (r)
      false
    }}

  /** A writer wants to acquire the lock; the lock provides a lower bound, and the writer must
    * commit strictly after that timestamp.
    *
    * If the writer may proceed immediately, returns Some (forecast).  Otherwise, it queues the
    * writer to be called back later.
    */
  def write (w: LockWriter): Option [TxClock] = synchronized {
    if (engaged == null) {
      if (forecast < w.ft)
        forecast = w.ft
      engaged = w
      Some (forecast)
    } else {
      writers.add (w)
      None
    }}

  /** A writer is finished with the lock.  If there are any waiting readers, raise the forecast to
    * the maximum read timestamp of all of them, and then let all of them proceed.  Next, if there
    * are waiting writers, let one proceed with that forecast.
    */
  def release (w0: LockWriter): Unit = {
    require (engaged == w0, "The writer releasing the lock does not hold it.")
    var rs = NoReaders
    var w = Option.empty [LockWriter]
    var ft = TxClock.MinValue
    synchronized {
      var rt = TxClock.MinValue
      var i = 0
      while (i < readers.length) {
        if (rt < readers(i).rt)
          rt = readers(i).rt
        i += 1
      }
      if (forecast < rt)
        forecast = rt
      if (!readers.isEmpty) {
        rs = readers
        readers = new util.ArrayList
      }
      if (!writers.isEmpty) {
        val _w = writers.remove()
        w = Some (_w)
        if (forecast < _w.ft)
          forecast = _w.ft
        engaged = _w
      } else {
        engaged = null
      }
      ft = forecast
    }
    rs foreach (_.grant())
    w foreach (_.grant (ft))
  }

  override def toString = s"Lock (ft=${forecast})"
}
