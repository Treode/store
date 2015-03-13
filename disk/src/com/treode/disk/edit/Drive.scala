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

import com.treode.async.Async, Async.{guard, supply}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.disk.{DiskConfig, DriveDigest, DriveGeometry, quote}

import SuperBlock.Common

private class Drive (
  file: File,
  geom: DriveGeometry,
  private var _draining: Boolean,
  val id: Int,
  val path: Path
) (
  implicit config: DiskConfig
) {

  def draining = _draining

  /** Called when launch completes. */
  def launch(): Unit = ()

  def writeSuperblock (common: Common): Async [Unit] =
    guard {
      val superb = SuperBlock (id, geom, _draining, common)
      SuperBlock.write (file, superb)
    }

  def startDraining(): Async [Unit] =
    supply {
      _draining = true
    }

  def awaitDrainStarted(): Async [Drive] =
    supply {
      this
    }

  def digest: DriveDigest =
    new DriveDigest (path, geom, 0, draining)

  def close(): Unit =
    file.close()

  override def toString = s"Drive(${quote (path)})"
}
