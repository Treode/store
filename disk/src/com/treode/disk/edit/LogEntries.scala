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

package com.treode.disk.edit

import com.treode.async.Async, Async.supply

case class LogEntries (batch: Long, entries: Seq [Unit => Any]) extends Ordered [LogEntries] {

  def compare (that: LogEntries): Int  =
    this.batch compare that.batch
}

object LogEntries extends Ordering [LogEntries] {

  val empty = LogEntries (0, Seq.empty)

  def compare (x: LogEntries, y: LogEntries): Int =
    x compare y
}
