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

import scala.collection.immutable.Queue

import com.treode.async.{Async, Callback, Fiber}
import com.treode.async.implicits._

import Async.guard
import Callback.{fanout, ignore}

private class Checkpointer (kit: DiskKit) {
  import kit.{config, drives, scheduler}

  val fiber = new Fiber
  var checkpointers: CheckpointerRegistry = null
  var bytes = 0
  var entries = 0
  var checkreqs = List.empty [Callback [Unit]]
  var engaged = true

  private def reengage() {
    fanout (checkreqs) .pass (())
    checkreqs = List.empty
    if (config.checkpoint (this.bytes, this.entries))
      _checkpoint()
    else
      engaged = false
  }

  private def _checkpoint() {
    guard {
      engaged = true
      bytes = 0
      entries = 0
      for {
        marks <- drives.mark()
        _ <- checkpointers.checkpoint()
        _ <- drives.checkpoint (marks)
      } yield fiber.execute {
        reengage()
      }
    } .run (ignore)
  }

  def launch (checkpointers: CheckpointerRegistry): Async [Unit] =
    fiber.supply {
      this.checkpointers = checkpointers
      if (!checkreqs.isEmpty || config.checkpoint (bytes, entries))
        _checkpoint()
      else
        engaged = false
    }

  def checkpoint(): Async [Unit] =
    fiber.async { cb =>
      checkreqs ::= cb
      if (!engaged)
        _checkpoint()
    }

  def tally (bytes: Int, entries: Int): Unit =
    fiber.execute {
      this.bytes += bytes
      this.entries += entries
      if (!engaged && checkreqs.isEmpty && config.checkpoint (this.bytes, this.entries))
        _checkpoint()
    }}
