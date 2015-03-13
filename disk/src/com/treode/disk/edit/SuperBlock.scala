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

import com.treode.async.Async, Async.guard
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.disk.{Disk, DiskConfig, DriveGeometry}

import SuperBlock.Common

/** The preamble on every disk. */
private case class SuperBlock (
  id: Int,
  geom: DriveGeometry,
  draining: Boolean,
  common: Common
)

private object SuperBlock {

  /** The common portion of the superblock; every disk holds the same values. */
  case class Common (gen: Int, dno: Int, paths: Set [Path])

  object Common {

    val empty = Common (0, 0, Set.empty)

    val pickler = {
      import DiskPicklers._
      wrap (int, int, set (path))
      .build ((Common.apply _).tupled)
      .inspect (v => (v.gen, v.dno, v.paths))
    }
  }

  val pickler = {
    import DiskPicklers._
    val common = Common.pickler
    wrap (int, driveGeometry, boolean, common)
    .build ((SuperBlock.apply _).tupled)
    .inspect (v => (v.id, v.geom, v.draining, v.common))
  }

  /** Read and unpickle the superblock. */
  def read (file: File) (implicit config: DiskConfig): Async [SuperBlock] =
    guard {
      val buf = PagedBuffer (12)
      for {
        _ <- file.fill (buf, 0, config.superBlockBytes)
      } yield {
        pickler.unpickle (buf)
      }}

  /** Pickle and write the superblock. */
  def write (file: File, sb: SuperBlock) (implicit config: DiskConfig): Async [Unit] =
    guard {
      val buf = PagedBuffer (12)
      pickler.pickle (sb, buf)
      assert (buf.writePos <= config.superBlockBytes)
      buf.writePos = sb.geom.blockAlignUp (buf.writePos)
      file.flush (buf, 0)
    }}
