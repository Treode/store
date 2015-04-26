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

import java.util.ArrayList

import com.treode.async.{Async, AsyncQueue, Callback, Fiber, Scheduler}
import com.treode.async.implicits._

private class Checkpointer (
  drives: DriveGroup,
  checkpoints: ArrayList [Unit => Async [Unit]]
) (implicit
  scheduler: Scheduler
) {

  private val fiber = new Fiber
  private val queue = new AsyncQueue (fiber) (reengage _)
  private var requests = List.empty [Callback [Unit]]
  private var running = List.empty [Callback [Unit]]

  queue.launch()

  private def reengage() {
    if (!requests.isEmpty)
      _checkpoint()
  }

  private def _checkpoint(): Unit =
    queue.begin {
      assert (running.isEmpty)
      running = requests
      requests = List.empty
      for {
        _ <- drives.startCheckpoint()
        _ <- checkpoints.latch (_(()))
        _ <- drives.finishCheckpoint()
      } yield {
        assert (!running.isEmpty)
        for (cb <- running)
          scheduler.pass (cb, ())
        running = List.empty
      }}

  def checkpoint(): Async [Unit] =
    fiber.async { cb =>
      requests ::= cb
      queue.engage()
    }}
