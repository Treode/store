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

import com.treode.async.{AsyncIterator, Callback}

private object Filters {

  /** Preserves first cell for key and time and eliminates subsequent ones.  Expects the input
    * iterator to be sorted by key and time.
    */
  def dedupe (iter: CellIterator): CellIterator = {
    var prev: Key = null
    iter.filter { cell =>
      val key = cell.timedKey
      if (key == prev) {
        false
      } else {
        prev = key
        true
      }}}

  /** Keep all that are newer than the limit; keep only the one newest that's older than the
    * limit.  Expects the input iterator to be sorted by key and reverse sorted by time.
    */
  def retire (iter: CellIterator, limit: TxClock): CellIterator = {
    var key = Option.empty [Bytes]
    iter.filter { cell =>
      if (cell.time >= limit) {
        key = None
        true
      } else if (key.isEmpty || cell.key != key.get) {
        key = Some (cell.key)
        true
      } else {
        false
      }}}}
