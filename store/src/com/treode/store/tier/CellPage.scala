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

package com.treode.store.tier

import java.util.{Arrays, ArrayList}

import com.treode.pickle.{Pickler, Picklers, PickleContext, UnpickleContext}
import com.treode.store.{Bound, Bytes, Cell, Key, TxClock}

private class CellPage (val entries: Array [Cell]) extends TierPage {

  def get (i: Int): Cell =
    entries (i)

  def ceiling (start: Bound [Key]): Int = {
    val target = Cell (start.bound.key, start.bound.time, None)
    val i = Arrays.binarySearch (entries, target, Cell)
    if (i < 0)
      -i-1
    else if (start.inclusive && i < entries.length)
      i
    else
      i+1
  }

  def size: Int = entries.size

  def isEmpty: Boolean = entries.size == 0

  def last: Cell = entries (entries.size - 1)

  override def toString = {
    val first = entries.head
    val last = entries.last
    s"CellPage(${first.key}:{first.time}, ${last.key}:${last.time})"
  }}

private object TierCellPage {

  val empty = new CellPage (new Array (0))

  def apply (entries: Array [Cell]): CellPage =
    new CellPage (entries)

  def apply (entries: ArrayList [Cell]): CellPage =
    new CellPage (entries.toArray (empty.entries))

  val pickler: Pickler [CellPage] =
    new AbstractPagePickler [CellPage, Cell] {

      private [this] val time = TxClock.pickler
      private [this] val value = Picklers.option (Bytes.pickler)

      protected def writeEntry (entry: Cell, ctx: PickleContext) {
        writeKey (entry.key, ctx)
        time.p (entry.time, ctx)
        value.p (entry.value, ctx)
      }

      protected def readEntry (ctx: UnpickleContext): Cell =
        Cell (readKey (ctx), time.u (ctx), value.u (ctx))

      protected def writeEntry (prev: Cell, entry: Cell, ctx: PickleContext) {
        writeKey (prev.key, entry.key, ctx)
        time.p (entry.time, ctx)
        value.p (entry.value, ctx)
      }

      protected def readEntry (prev: Cell, ctx: UnpickleContext): Cell =
        Cell (readKey (prev.key, ctx), time.u (ctx), value.u (ctx))

      def p (page: CellPage, ctx: PickleContext): Unit =
        _p (page.entries, ctx)

      def u (ctx: UnpickleContext): CellPage =
        new CellPage (_u (ctx))
  }}
