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

import com.google.common.primitives.Longs
import com.treode.store.{Bytes, StorePicklers, TxClock}
import com.treode.disk.Position

private case class IndexEntry (key: Bytes, time: TxClock, disk: Int, offset: Long, length: Int)
extends Ordered [IndexEntry] {

  def pos = Position (disk, offset, length)

  def byteSize = IndexEntry.pickler.byteSize (this)

  def compare (that: IndexEntry): Int = {
    val r = key compare that.key
    if (r != 0) return r
    // Reverse chronological order
    that.time compare time
  }}

private object IndexEntry extends Ordering [IndexEntry] {

  def apply (key: Bytes, time: TxClock, pos: Position): IndexEntry =
    new IndexEntry (key, time, pos.disk, pos.offset, pos.length)

  def compare (x: IndexEntry, y: IndexEntry): Int =
    x compare y

  val pickler = {
    import StorePicklers._
    wrap (bytes, txClock, uint, ulong, uint)
    .build (v => IndexEntry (v._1, v._2, v._3, v._4, v._5))
    .inspect (v => (v.key, v.time, v.disk, v.offset, v.length))
  }}
