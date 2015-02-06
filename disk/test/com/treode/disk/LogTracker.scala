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

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.implicits._

import Async.{guard, supply}
import DiskTestTools._
import LogTracker.{pagers, records}

class LogTracker {
  self =>

  private var attempted = Map.empty [Int, Int] .withDefaultValue (-1)
  private var accepted = Map.empty [Int, Int] .withDefaultValue (-1)
  private var gen = 0

  def put (n: Int, k: Int, v: Int) (implicit disk: Disk): Async [Unit] = {
    attempted += k -> v
    val g = gen
    for {
      _ <- records.put.record (n, g, k, v)
    } yield {
      accepted += k -> v
    }}

  def batch (nkeys: Int, round: Int, nputs: Int) (
      implicit random: Random, disk: Disk): Async [Unit] =
    for (k <- random.nextInts (nputs, nkeys) .latch)
      put (round, k, random.nextInt (1<<20))

  def batches (nkeys: Int, nbatches: Int, nputs: Int, start: Int = 0) (
      implicit random: Random, scheduler: Scheduler, disk: Disk): Async [Unit] =
    for {
      n <- (0 until nbatches) .async
      k <- random.nextInts (nputs, nkeys) .latch
    } {
      put (n + start, k, random.nextInt (1<<20))
    }

  def checkpoint () (implicit disk: Disk): Async [Unit] =
    guard {
      val save = attempted
      val g = gen
      gen += 1
      for {
        pos <- pagers.table.write (0, g, save)
        _ <- records.checkpoint.record (g, pos)
      } yield ()
    }

  def probe (groups: Set [Int]) (implicit scheduler: Scheduler): Async [Set [Int]] =
    supply (groups)

  def compact () (implicit disk: Disk): Async [Unit] =
    guard {
      checkpoint()
    }

  def attach () (implicit scheduler: Scheduler, launch: Disk.Launch) {
    import launch.disk

    launch.checkpoint (checkpoint())

    pagers.table.handle (new PageHandler [Int] {

      def probe (obj: ObjectId, groups: Set [Int]): Async [Set [Int]] =
        self.probe (groups)

      def compact (obj: ObjectId, groups: Set [Int]): Async [Unit] =
        self.checkpoint()
    })
  }

  def check (found: Map [Int, Int]) {
    for (k <- accepted.keySet)
      assert (found contains k)
    for ((k, v) <- found)
      assert (attempted (k) == v || accepted (k) == v)
  }

  override def toString = s"Tracker(\n  $attempted,\n  $accepted)"
}

object LogTracker {

  object records {
    import DiskPicklers._

    val put =
      RecordDescriptor (0xBF, tuple (uint, uint, uint, uint))

    val checkpoint =
      RecordDescriptor (0x9B, tuple (uint, pos))
  }

  object pagers {
    import DiskPicklers._

    val table =
      PageDescriptor (0x79, uint, map (uint, uint))
  }}
