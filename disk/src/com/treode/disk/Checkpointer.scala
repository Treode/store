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

import com.treode.async.{Async, AsyncQueue, Callback, Fiber, Scheduler}

private class Checkpointer (
  drives: DriveGroup,

  // The total bytes of records since our last checkpoint.
  private var bytes: Long,

  // The count of records since our last checkpoint.
  private var entries: Long
) (implicit
  scheduler: Scheduler,
  config: DiskConfig
) {

  private val fiber = new Fiber
  private val queue = new AsyncQueue (fiber) (reengage _)
  private var requests = List.empty [Callback [Unit]]
  private var running = List.empty [Callback [Unit]]
  private var checkpoints: CheckpointerRegistry = null

  private def reengage() {
    if (checkpoints == null)
      return
    if (config.checkpoint (bytes, entries) || !requests.isEmpty)
      _checkpoint()
  }

  private def _checkpoint(): Unit =
    queue.begin {
      assert (running.isEmpty)
      running = requests
      requests = List.empty
      bytes = 0L
      entries = 0L
      for {
        _ <- drives.startCheckpoint()
        _ <- checkpoints.checkpoint()
        _ <- drives.finishCheckpoint()
      } yield {
        for (cb <- running)
          scheduler.pass (cb, ())
        running = List.empty
      }}

  def launch (checkpoints: CheckpointerRegistry): Unit =
    fiber.execute {
      this.checkpoints = checkpoints
      queue.engage()
    }

  /** Tally counts for records recently logged; maybe trigger a checkpoint. */
  def tally (bytes: Long, entries: Long): Unit =
    fiber.execute {
      this.bytes += bytes
      this.entries += entries
      queue.engage()
    }

  /** Force a checkpoint; for testing. */
  def checkpoint(): Async [Unit] =
    fiber.async { cb =>
      requests ::= cb
      queue.engage()
    }}
