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

import com.treode.buffer.{ArrayBuffer, PagedBuffer}
import com.treode.pickle.PicklerRegistry

private class RecordRegistry {

  private val records =
    PicklerRegistry [Unit => Any] ("RecordRegistry")

  def replay [R] (desc: RecordDescriptor [R]) (f: R => Any): Unit =
    records.register (desc.prec, desc.id.id) (msg => _ => f (msg))

  def read (id: TypeId, data: Array [Byte]): Unit => Any =
    records.unpickle (id.id, ArrayBuffer.readable (data))

  def read (id: Long, buf: PagedBuffer, len: Int): Unit => Any =
    records.unpickle (id, buf, len)
}

private object RecordRegistry {

  def frame [R] (desc: RecordDescriptor [R], time: Long, entry: R, buf: PagedBuffer): Unit = {
    import DiskPicklers.tuple
    import RecordHeader.{Entry, pickler}
    tuple (pickler, desc.prec) .frame ((Entry (time, desc.id), entry), buf)
  }}
