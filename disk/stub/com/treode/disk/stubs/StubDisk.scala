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

package com.treode.disk.stubs

import scala.collection.mutable.UnrolledBuffer
import scala.util.{Failure, Random, Success}

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.misc.EpochReleaser
import com.treode.disk._

import Async.{async, guard, latch, supply}
import Callback.{fanout, ignore}

private class StubDisk (
    releaser: EpochReleaser
) (implicit
    random: Random,
    scheduler: Scheduler,
    disk: StubDiskDrive,
    config: StubDiskConfig
) extends Disk {

  val logd = new Dispatcher [(StubRecord, Callback [Unit])]
  val checkpointer = new StubCheckpointer
  val compactor = new StubCompactor (releaser)

  logd.receive (receiver _)

  def align (n: Int): Int = {
    val bits = 6
    val mask = (1 << bits) - 1
    (n + mask) & ~mask
  }

  def receiver (batch: Long, records: UnrolledBuffer [(StubRecord, Callback [Unit])]) {
    logd.send (new UnrolledBuffer [(StubRecord, Callback [Unit])])
    val cb = fanout (records .map (_._2))
    disk.log (records .map (_._1) .toSeq) .run { v =>
      logd.receive (receiver _)
      cb (v)
    }}

  def launch (checkpoints: CheckpointerRegistry, pages: StubPageRegistry) {
    checkpointer.launch (checkpoints)
    compactor.launch (pages)
  }

  def record [R] (desc: RecordDescriptor [R], entry: R): Async [Unit] =
    async { cb =>
      logd.send ((StubRecord (desc, entry), cb))
      checkpointer.tally()
    }

  def read [P] (desc: PageDescriptor [P], pos: Position): Async [P] =
    guard {
      for {
        page <- disk.read (pos.offset)
      } yield {
        require (align (page.length) == pos.length)
        desc.ppag.fromByteArray (page.data)
      }}

  def write [P] (desc: PageDescriptor [P], obj: ObjectId, gen: Long, page: P): Async [Position] =
    guard {
      val _page = StubPage (desc, obj, gen, page)
      for {
        offset <- disk.write (_page)
      } yield {
        compactor.tally()
        Position (0, offset, align (_page.length))
      }}

  def compact (desc: PageDescriptor [_], obj: ObjectId): Unit =
    compactor.compact (desc.id, obj)

  def join [A] (task: Async [A]): Async [A] =
    releaser.join (task)
}

object StubDisk {

  /** This has been moved to package level for easier access in the Scaladoc. */
  @deprecated ("Use StubDiskConfig", "0.3.0")
  type Config = StubDiskConfig

  /** This has been moved to package level for easier access in the Scaladoc. */
  @deprecated ("Use StubDiskConfig", "0.3.0")
  val Config = StubDiskConfig

  /** This has been moved to package level for easier access in the Scaladoc. */
  @deprecated ("Use StubDiskRecovery", "0.3.0")
  type Recovery = StubDiskRecovery

  def recover () (implicit
      random: Random,
      scheduler: Scheduler,
      config: StubDiskConfig
  ): StubDiskRecovery =
    new StubRecoveryAgent () (random, scheduler, config)
}
