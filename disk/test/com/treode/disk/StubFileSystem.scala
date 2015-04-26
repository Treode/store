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

import java.nio.file.{OpenOption, Path}

import com.treode.async.Scheduler
import com.treode.async.io.File
import com.treode.async.io.stubs.StubFile

/** A FileSystem that uses StubFiles. */
private class StubFileSystem extends FileSystem {

  private case class Entry (data: Array [Byte], align: Int)

  private var directory = Map.empty [Path, Entry]

  /** Create a stub file; invoke before [[#open]]. */
  def create (path: Path, size: Int, align: Int) {
    require (!(directory contains path), s"File $path already exists.")
    directory += path -> (new Entry (new Array (size), align))
  }

  /** Create stub files; invoke before [[#open]]. */
  def create (paths: Seq [Path], size: Int, align: Int): Unit =
    paths foreach (create (_, size, align))

  /** Opens the file if it exists; use [[#create]] to ensure it exists. Ignores `opts`. */
  def open (path: Path, opts: OpenOption*) (implicit scheduler: Scheduler): File = {
    require (directory contains path, s"File $path does not exist.")
    val entry = directory (path)
    StubFile (entry.data, entry.align)
  }}
