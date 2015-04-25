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

import com.treode.async.{Async, Scheduler}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.disk.DriveGeometry

class LogReader (
  file: File,
  geom: DriveGeometry
) (implicit
  scheduler: Scheduler
) {

  var buf = PagedBuffer (12)
  var pos = 0L

  def read(): Async [Seq [String]] = {
    val builder = Seq.newBuilder [String]
    var continue = true
    scheduler.whilst (continue) {
      for {
        _ <- file.fill (buf, pos, 8)
        // get byte length of batch
        length = buf.readInt()
        // get batch count of items in batch
        count = buf.readInt()
        // read lengt` bytes
        _ <- file.fill (buf, pos+8, length)
      } yield {
        for (i <- 1 to count)
          builder += buf.readString()

        pos += 8 + length
        if (buf.readByte() == 0)
          continue = false
      }
    } map {
      _ => builder.result()
    }}}
