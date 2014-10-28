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

package com.treode.store

import com.treode.async.AsyncIterator

/** A window of time for scanning a table.
  *
  * TreodeDB retains past values for rows, and windows permit the scanning of just one recent
  * value for the row or its changes over time.  The case classes are nested in the
  * [[Window$ companion object]].
  *
  * <img src="../../../img/windows.png"/>
  */
sealed abstract class Window {

  def later: Bound [TxClock]

  def filter (iter: AsyncIterator [Cell]): AsyncIterator [Cell]
}

/** A window of time for scanning a table.
  *
  * TreodeDB retains past values for rows, and windows permit the scanning of just one recent
  * value for the row or its changes over time.
  *
  * <img src="../../../img/windows.png"/>
  */
object Window {

  /** Choose only the latest value as of `later` so long as that was set after `earlier`. */
  case class Latest (later: Bound [TxClock], earlier: Bound [TxClock]) extends Window {

    def filter (iter: AsyncIterator [Cell]): AsyncIterator [Cell] = {
      var key = Option.empty [Bytes]
      iter.filter { cell =>
        if (later >* cell.time && earlier <* cell.time && (key.isEmpty || cell.key != key.get)) {
          key = Some (cell.key)
          true
        } else {
          false
        }}}}

  object Latest {

    def now = Latest (TxClock.now, true)

    def apply (later: TxClock, linc: Boolean, earlier: TxClock, einc: Boolean): Latest =
      Latest (Bound (later, linc), Bound (earlier, einc))

    def apply (later: TxClock, inclusive: Boolean): Latest =
      Latest (Bound (later, inclusive), Bound (TxClock.MinValue, true))
  }

  @deprecated ("Use Latest", "0.2.0")
  class Recent (later: Bound [TxClock], earlier: Bound [TxClock]) extends Latest (later, earlier)

  @deprecated ("Use Latest", "0.2.0")
  object Recent {

    def now = Recent (TxClock.now, true)

    def apply (later: Bound [TxClock], earlier: Bound [TxClock]): Recent =
      new Recent (later, earlier)

    def apply (later: TxClock, linc: Boolean, earlier: TxClock, einc: Boolean): Recent =
      Recent (Bound (later, linc), Bound (earlier, einc))

    def apply (later: TxClock, inclusive: Boolean): Recent =
      Recent (Bound (later, inclusive), Bound (TxClock.MinValue, true))
  }

  /** Choose all changes between `later` and `earlier`. */
  case class Between (later: Bound [TxClock], earlier: Bound [TxClock]) extends Window {

    def filter (iter: AsyncIterator [Cell]): AsyncIterator [Cell] =
      iter.filter (cell => later >* cell.time && earlier <* cell.time)
  }

  object Between {

    def apply (later: TxClock, linc: Boolean, earlier: TxClock, einc: Boolean): Between =
      Between (Bound (later, linc), Bound (earlier, einc))
  }

  /** Choose all changes between `later` and `earlier` and the most recent change as of
    * `earlier`.
    */
  case class Through (later: Bound [TxClock], earlier: TxClock) extends Window {

    def filter (iter: AsyncIterator [Cell]): AsyncIterator [Cell] = {
      var key = Option.empty [Bytes]
      iter.filter { cell =>
        if (later >* cell.time && earlier < cell.time) {
          key = None
          true
        } else if (earlier >= cell.time && (key.isEmpty || cell.key != key.get)) {
          key = Some (cell.key)
          true
        } else {
          false
        }}}}

  object Through {

    def apply (later: TxClock, linc: Boolean, earlier: TxClock): Through =
      Through (Bound (later, linc), earlier)
  }

  val all = Between (TxClock.MaxValue, true, TxClock.MinValue, true)

  val pickler = {
    import StorePicklers._
    tagged [Window] (
      0x1 -> wrap (tuple (bound (txClock), bound (txClock)))
          .build (v => new Latest (v._1, v._2))
          .inspect (v => (v.later, v.earlier)),
      0x2 -> wrap (tuple (bound (txClock), bound (txClock)))
          .build (v => (new Between (v._1, v._2)))
          .inspect (v => (v.later, v.earlier)),
      0x3 -> wrap (tuple (bound (txClock), txClock))
          .build (v => (new Through (v._1, v._2)))
          .inspect (v => (v.later, v.earlier)))
  }}
