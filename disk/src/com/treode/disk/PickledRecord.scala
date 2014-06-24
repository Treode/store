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

package com.treode.disk

import com.treode.async.Callback
import com.treode.buffer.PagedBuffer

private trait PickledRecord {

  def cb: Callback [Unit]
  def byteSize: Int
  def disk: Option [Int]
  def write (num: Long, buf: PagedBuffer)
}

private object PickledRecord {

  private class Header (entry: RecordHeader, val disk: Option [Int], val cb: Callback [Unit])
  extends PickledRecord {

    val byteSize =
      RecordHeader.pickler.byteSize (entry)

    def write (num: Long, buf: PagedBuffer) =
      RecordHeader.pickler.frame (entry, buf)

    override def toString =
      s"PickledRecord($entry)"
  }

  def apply (disk: Int, entry: RecordHeader, cb: Callback [Unit]): PickledRecord =
    new Header (entry, Some (disk), cb)

  private class Entry [R] (desc: RecordDescriptor [R], entry: R, val cb: Callback [Unit])
  extends PickledRecord {

    val byteSize =
      RecordHeader.overhead + desc.prec.byteSize (entry)

    def disk = None

    def write (num: Long, buf: PagedBuffer) =
      RecordRegistry.frame (desc, num, entry, buf)

    override def toString =
      s"PickledRecord(${desc.id}, $entry)"
  }

  def apply [R] (desc: RecordDescriptor [R], entry: R, cb: Callback [Unit]): PickledRecord =
    new Entry (desc, entry, cb)
}
