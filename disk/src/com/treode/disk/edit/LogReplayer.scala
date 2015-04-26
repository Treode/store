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

import com.treode.async.{Async, Scheduler}, Async.supply
import com.treode.disk.{DiskEvents, FileSystem}

private class LogReplayer (implicit
  files: FileSystem,
  scheduler: Scheduler,
  events: DiskEvents
) {

  private var _batch = 0L
  private var _drives = Map.empty [Int, Drive]

  def batch: Long =
    synchronized (_batch)

  def drives: Map [Int, Drive] =
    synchronized (_drives)

  def raise (batch: Long): Unit =
    synchronized {
      if (_batch < batch)
        _batch = batch
    }

  def reattach (drive: Drive): Unit =
    synchronized (_drives += drive.id -> drive)

  def replay (xss: Iterable [LogEntries]): Async [Unit] =
    supply {
      for (xs <- xss) {
        raise (xs.batch)
        for (x <- xs.entries)
          x (())
      }}}
