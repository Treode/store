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

private class PageReader (
  val file: File
) {

  val bits = 10
  val buffer = PagedBuffer (bits)

  /**
   * Returns (if successful) the string of length `length` at `pos` in the
   * file asynchronously, using a read buffer.
   */
  def readString (pos: Long, length: Int) : Async [String] = {
    buffer.clear()
    for {
      _ <- file.fill (buffer, pos, length)
    } yield {
      buffer.readString ()
    }
  }
}
