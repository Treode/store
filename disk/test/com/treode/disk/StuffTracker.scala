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
import com.treode.async.stubs.implicits._
import com.treode.async.stubs.StubScheduler

import Async.{guard, supply}
import DiskTestTools._
import Stuff.pager

class StuffTracker (implicit random: Random) {

  private var seeds = Set.empty [Long]
  private var written = Map.empty [Long, Position]
  private var _probed = false
  private var _compacted = false
  private var _maximum = 0L

  def probed = _probed
  def compacted = _compacted
  def maximum = _maximum

  def write() (implicit disk: Disk): Async [Unit] = {
    var seed = random.nextLong()
    while (written contains seed)
      seed = random.nextLong()
    seeds += seed
    for {
      pos <- pager.write (0, seed, Stuff (seed))
    } yield {
      if (pos.offset > _maximum)
        _maximum = pos.offset
      written += seed -> pos
    }}

  def batch (nbatches: Int, nwrites: Int) (implicit scheduler: Scheduler, disk: Disk): Async [Unit] =
    for {
      _ <- (0 until nbatches) .async
      _ <- (0 until nwrites) .latch
    } {
      write() .flatMap (_ => scheduler.sleep (1))
    }

  def attach () (implicit scheduler: Scheduler, launch: Disk.Launch) {
    import launch.disk

    _probed = false
    _compacted = false

    pager.handle (new PageHandler [Long] {

      def probe (obj: ObjectId, groups: Set [Long]): Async [Set [Long]] =
        supply {
          _probed = true
          val (keep, remove) = groups partition { s =>
            !(written contains s) || random.nextInt (3) == 0
          }
          written --= remove
          keep
        }

      def compact (obj: ObjectId, groups: Set [Long]): Async [Unit] = {
        guard {
          _compacted = true
          for (seed <- groups.latch)
            for (pos <- pager.write (0, seed, Stuff (seed)))
              yield written += seed -> pos
        }}})
  }

  def check () (implicit scheduler: StubScheduler, disk: Disk) {
    for ((seed, pos) <- written) {
      if (disk.isInstanceOf [DiskAgent])
        pager.assertInLedger (pos, 0, seed)
      pager.read (pos) .expectPass (Stuff (seed))
    }}}
