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

import scala.util.{Failure, Success}

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.misc.EpochReleaser

import Async.{guard, latch}
import Callback.ignore

private class DiskKit (
    val sysid: Array [Byte],
    logBatch: Long
) (implicit
    val scheduler: Scheduler,
    val config: Disk.Config
) {

  val logd = new Dispatcher [PickledRecord]
  logd.batch = logBatch
  val paged = new Dispatcher [PickledPage]
  val drives = new DiskDrives (this)
  val checkpointer = new Checkpointer (this)
  val releaser = new EpochReleaser
  val compactor = new Compactor (this)

  def launch (checkpoints: CheckpointRegistry, pages: PageRegistry): Unit =
    guard {
      for {
        _ <- latch (
            checkpointer.launch (checkpoints),
            compactor.launch (pages))
        _ <- drives.launch()
      } yield ()
    } run (ignore)

  def close(): Async [Unit] =
    for {
      _ <- compactor.close()
      _ <- drives.close()
    } yield ()
}
