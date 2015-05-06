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

import java.nio.file.Path
import java.util.ArrayDeque

import com.treode.async.{Async, Scheduler}, Async.guard
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.disk.{DiskConfig, DriveGeometry}

private class BootstrapDrive (
  val id: Int,
  path: Path,
  file: File,
  geom: DriveGeometry,
  draining: Boolean,
  alloc: SegmentAllocator,
  logBuf: PagedBuffer,
  logSegs: ArrayDeque [Int],
  logPos: Long,
  logLimit: Long,
  logHead: Long,
  flushed: Long
) (implicit
  scheduler: Scheduler,
  config: DiskConfig
) {

  def read (offset: Long, length: Int): Async [PagedBuffer] =
    guard {
      val buf = PagedBuffer (12)
      for {
        _ <- file.fill (buf, offset, length)
      } yield {
        buf
      }}

  def result (logdsp: LogDispatcher, pagdsp: PageDispatcher, ledger: SegmentLedger): Drive = {
    val logdtch = new Detacher (draining)
    val logwrtr =
      new LogWriter (path, file, geom, logdsp, logdtch, alloc, logBuf, logSegs, logPos, logLimit, logHead, flushed)
    val pagdtch = new Detacher (draining)
    val pagwrtr = new PageWriter (id, path, file, geom, pagdsp, pagdtch, alloc, ledger)
    new Drive (file, geom, alloc, logwrtr, pagwrtr, draining, id, path)
  }}
