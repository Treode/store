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

package com.treode.store.stubs

import com.treode.store.{Bytes, TableId, TxClock}

private case class StubKey (table: TableId, key: Bytes, time: TxClock)
extends Ordered [StubKey] {

  def compare (that: StubKey): Int = {
    var r = table compare that.table
    if (r != 0) return r
    r = key compare that.key
    if (r != 0) return r
    // Reverse chronological order.
    that.time compare time
  }}

private object StubKey extends Ordering [StubKey] {

  def compare (x: StubKey, y: StubKey): Int =
    x compare y
}
