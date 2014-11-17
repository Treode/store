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

import com.treode.async.BatchIterator
import com.treode.pickle.Pickler

import WriteOp.{Create, Delete, Hold, Update}

@deprecated ("use alt.TableDescriptor", "0.2.0")
trait Accessor [K, V] {

  case class ACell (key: K, time: TxClock, value: Option [V])

  def read (k: K): ReadOp

  def create (k: K, v: V): Create
  def hold (k: K): Hold
  def update (k: K, v: V): Update
  def delete (k: K): Delete

  def cell (c: Cell): ACell
  def value (v: Value): Option [V]

  def scan (
      start: Bound [(K, TxClock)],
      window: Window,
      slice: Slice
  ) (implicit
      store: Store
  ): BatchIterator [ACell]

  def scan (window: Window, slice: Slice) (implicit store: Store): BatchIterator [ACell]

  def recent (rt: TxClock) (implicit store: Store): BatchIterator [ACell]
}

@deprecated ("Use Transaction", "0.2.0")
object Accessor {

  def apply [K, V] (id: TableId, pk: Pickler [K], pv: Pickler [V]): Accessor [K, V] =
    new Accessor [K, V] {

      def read (k: K)         = ReadOp (id, Bytes (pk, k))

      def create (k: K, v: V) = Create (id, Bytes (pk, k), Bytes (pv, v))
      def hold (k: K)         = Hold (id, Bytes (pk, k))
      def update (k: K, v: V) = Update (id, Bytes (pk, k), Bytes (pv, v))
      def delete (k: K)       = Delete (id, Bytes (pk, k))

      def cell (c: Cell)      = ACell (c.key (pk), c.time, c.value (pv))
      def value (v: Value)    = v.value (pv)

      def scan (
          start: Bound [(K, TxClock)],
          window: Window,
          slice: Slice
      ) (implicit
          store: Store
      ): BatchIterator [ACell] = {
        val (key, time) = start.bound
        val _start = Bound (Key (Bytes (pk, key), time), start.inclusive)
        store.scan (id, _start, window, slice) .map (cell _)
      }

      def scan (window: Window, slice: Slice) (implicit store: Store): BatchIterator [ACell] =
        store.scan (id, Bound.firstKey, window, slice) .map (cell _)

      def recent (rt: TxClock) (implicit store: Store): BatchIterator [ACell] =
        scan (Window.Latest (rt, true), Slice.all)
    }}
