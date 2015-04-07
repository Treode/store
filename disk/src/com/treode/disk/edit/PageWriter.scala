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

import scala.collection.mutable.UnrolledBuffer

import com.treode.async.{Async, Callback}, Callback.ignore
import com.treode.async.implicits._
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.disk.{PickledPage, Position}

/** Receives items from a PageDispatcher and writes them to a file. */
private class PageWriter (dsp: PageDispatcher, file: File) {

  private val bits = 10
  private val buffer = PagedBuffer (bits)

  // We maintain an internal pointer into the file for where to write in order to preserve
  // consistency between writes.
  private var pos = 0L

  // We consistently check to see if we have anything to write.
  listen() run (ignore)

  /** Polls the dispatcher for items to write. */
  def listen(): Async [Unit] =
    for {
      (_, dataBuffers) <- dsp.receive()
      _ <- write (dataBuffers)
    } yield {
      listen() run (ignore)
    }

    /* Write the PickledPages to the file, then pass the positions to the callbacks inside the
     * PickledPages.
     */
   def write (data: UnrolledBuffer [PickledPage]): Async [Unit] = {

    var writePositions = new Array [Position] (0)
    var beforeAnyWrites = pos
    for (s <- data) {
      val beforeEachWritePos = buffer.writePos
      s.write (buffer)
      val writeLen = (buffer.writePos - beforeEachWritePos).toLong
      writePositions :+= (Position (0, pos.toLong, writeLen.toInt)) //only focus on one disk for now
      pos += writeLen
    }

    for {
      _ <- file.flush (buffer, beforeAnyWrites)
    } yield {
      buffer.clear()
      for ((s, p) <- data zip writePositions)
        s.cb.pass (p)
    }}}
