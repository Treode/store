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

import java.util.concurrent.atomic.AtomicLong
import com.treode.async.Async

import Async.{async, guard}

private class DiskAgent (val kit: DiskKit) extends Disk {
  import kit.{compactor, logd, paged, releaser}
  import kit.config.{maximumPageBytes, maximumRecordBytes}

  val cache = new PageCache (kit)

  def record [R] (desc: RecordDescriptor [R], entry: R): Async [Unit] =
    async { cb =>
      val _entry = PickledRecord (desc, entry, cb)
      if (_entry.byteSize > maximumRecordBytes)
        throw new OversizedRecordException (maximumRecordBytes, _entry.byteSize)
      logd.send (_entry)
    }

  def read [P] (desc: PageDescriptor [P], pos: Position): Async [P] =
    cache.read (desc, pos)

  def write [P] (desc: PageDescriptor [P], obj: ObjectId, group: GroupId, page: P): Async [Position] =
    async [Position] { cb =>
      val _page = PickledPage (desc, obj, group, page, cb)
      if (_page.byteSize > maximumPageBytes)
        throw new OversizedPageException (maximumPageBytes, _page.byteSize)
      paged.send (_page)
    } .map { pos =>
      cache.write (pos, page)
      pos
    }

  def compact (desc: PageDescriptor [_], obj: ObjectId): Async [Unit] =
    compactor.compact (desc.id, obj)

  def join [A] (task: Async [A]): Async [A] =
    releaser.join (task)
}
