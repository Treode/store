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

import com.treode.async.Async
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

import Async.guard

private case class SuperBlock (
    id: Int,
    boot: BootBlock,
    geometry: DriveGeometry,
    draining: Boolean,
    free: IntSet,
    logHead: Long)

private object SuperBlock {

  val pickler = {
    import DiskPicklers._
    // Tagged for forwards compatibility.
    tagged [SuperBlock] (
        0x0024811306495C5FL ->
            wrap (uint, boot, geometry, boolean, intSet, ulong)
            .build ((SuperBlock.apply _).tupled)
            .inspect (v => (
                v.id, v.boot, v.geometry, v.draining, v.free, v.logHead)))
  }

  def position (gen: Int) (implicit config: DiskConfig): Long =
    if ((gen & 0x1) == 0) 0L else config.superBlockBytes

  def clear (gen: Int, file: File) (implicit config: DiskConfig): Async [Unit] =
    guard {
      val buf = PagedBuffer (config.superBlockBits)
      while (buf.writePos < config.superBlockBytes)
        buf.writeInt (0)
      file.flush (buf, position (gen))
    }

  def write (superb: SuperBlock, file: File) (implicit config: DiskConfig): Async [Unit] =
    guard {
      val buf = PagedBuffer (config.superBlockBits)
      pickler.pickle (superb, buf)
      if (buf.writePos > config.superBlockBytes)
        throw new SuperblockOverflowException
      buf.writePos = config.superBlockBytes
      file.flush (buf, position (superb.boot.gen))
    }}
