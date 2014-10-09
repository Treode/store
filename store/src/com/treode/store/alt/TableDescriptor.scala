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

package com.treode.store.alt

import com.treode.async.AsyncIterator
import com.treode.store.{Cell => SCell, _}

/** A TableDescriptor ties together a [[com.treode.store.TableId TableId]], a [[Froster]] for the 
  * key, and a Froster for the value. It works with a [[Transaction]] to make reading and writing
  * the database convenient.
  */
class TableDescriptor [K, V] (val id: TableId, val key: Froster [K], val value: Froster [V]) {

  case class Cell (val key: K, time: TxClock, value: Option [V])

  object Cell {

    def apply (c: SCell): Cell =
      Cell (key.thaw (c.key), c.time, value.thaw (c.value))
  }

  def scan (
      start: Bound [(K, TxClock)],
      window: Window,
      slice: Slice
  ) (implicit
      store: Store
  ): AsyncIterator [Cell] = {
    val (key, time) = start.bound
    val _start = Bound (Key (this.key.freeze (key), time) , start.inclusive)
    store.scan (id, _start, window, slice) .map (Cell.apply (_))
  }

  def scan (
      start: K,
      window: Window,
      slice: Slice
  ) (implicit
      store: Store
  ): AsyncIterator [Cell] = {
    val _start = Bound (Key (this.key.freeze (start), TxClock.MaxValue) , true)
    store.scan (id, _start, window, slice) .map (Cell.apply (_))
  }

  def scan (
      window: Window,
      slice: Slice
  ) (implicit
      store: Store
  ): AsyncIterator [Cell] =
    store.scan (id, Bound.firstKey, window, slice) .map (Cell.apply (_))

  def scan () (implicit store: Store): AsyncIterator [Cell] =
    scan (Window.all, Slice.all)

  def recent (rt: TxClock, start: K) (implicit store: Store): AsyncIterator [Cell] =
    scan (start, Window.Recent (rt, true), Slice.all)

  def recent (rt: TxClock) (implicit store: Store): AsyncIterator [Cell] =
    scan (Window.Recent (rt, true), Slice.all)
}

object TableDescriptor {

  def apply [K, V] (id: TableId, k: Froster [K], v: Froster [V]): TableDescriptor [K, V] =
    new TableDescriptor (id, k, v)
}