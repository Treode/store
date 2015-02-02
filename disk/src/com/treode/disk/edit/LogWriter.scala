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

import scala.util.{Failure, Success}

import com.treode.async.{Async, Scheduler}, Async.async
import com.treode.async.implicits._
import com.treode.async.io.File
import com.treode.disk.DriveGeometry
import com.treode.buffer.PagedBuffer

class LogWriter (file: File, geom: DriveGeometry) (implicit scheduler: Scheduler) {

  import geom.{blockAlignUp, blockAlignDown}

  var pos = 0L
  val buf = PagedBuffer (10)

  def record (s: String): Async [Unit] =
    async { cb =>
      // read pos is aligned at start, and after each record
      // write pos is at end of last record
      if (buf.writePos > 0)
        // if there is already one record
        buf.writeByte (1)
      val start = buf.writePos
      buf.writeInt (0)  // make space for length
      buf.writeString (s)

      val end = buf.writePos // remember where we parked
      buf.writePos = start
      buf.writeInt (end - start - 4)    // string length in bytes
      buf.writePos = end
      buf.writeByte (0)
      buf.writePos = geom.blockAlignUp (buf.writePos)

      file.flush (buf, pos) run {
        case Success (length) => {
          // flush move readPos to == writePos
          buf.readPos = geom.blockAlignDown (end)
          buf.writePos = end
          pos += buf.discard (buf.readPos)
          cb.pass (())
        }
        case Failure (thrown) => cb.fail (thrown)
      }
    }
}
