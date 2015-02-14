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

import com.treode.async.io.File
import com.treode.async.Async
import com.treode.buffer.PagedBuffer

private class PageWriter (
  val file: File
) {

  val bits = 10
  val buffer = PagedBuffer (bits)
  var pos : Long = 0

  /**
   * Write `data` into the file asynchronously, using a write buffer. Returns
   * the position of the writer and length written, if successful.
   */
  def write (data: String) : Async [(Long, Int)] = {
    buffer.writeString (data)
    val len = buffer.writePos
    for {
      _ <- file.flush (buffer, pos)
    } yield {
      pos += buffer.writePos
      buffer.clear ()
      (pos, len)
    }
  }
}
