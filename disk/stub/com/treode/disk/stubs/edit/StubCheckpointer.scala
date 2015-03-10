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

package com.treode.disk.stubs.edit

import java.util.ArrayList

import com.treode.async.{Async, AsyncQueue, Callback, Fiber, Scheduler}
import com.treode.disk.CheckpointerRegistry
import com.treode.disk.stubs.StubDiskDrive

private class StubCheckpointer (
  drive: StubDiskDrive
) (implicit
  scheduler: Scheduler
) {

  private var checkpointers: CheckpointerRegistry = null

  private val fiber = new Fiber
  private val queue = new AsyncQueue (fiber) (reengage _)
  private var requests = List.empty [Callback [Unit]]
  private var running = List.empty [Callback [Unit]]

  queue.launch()

  private def reengage() {
    if (checkpointers == null)
      ()
    else if (!requests.isEmpty)
      _checkpoint()
  }

  private def _checkpoint(): Unit =
    queue.begin {
      assert (running.isEmpty)
      running = requests
      requests = List.empty
      val mark = drive.mark()
      for {
        _ <- checkpointers.checkpoint()
      } yield {
        drive.checkpoint (mark)
        assert (!running.isEmpty)
        for (cb <- running)
          scheduler.pass (cb, ())
        running = List.empty
      }}

  def launch (checkpointers: CheckpointerRegistry): Unit =
    fiber.execute {
      this.checkpointers = checkpointers
      queue.engage()
    }

  def checkpoint(): Async [Unit] =
    fiber.async { cb =>
      requests ::= cb
      queue.engage()
    }}
