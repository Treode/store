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
import Disk.{Launch, Recovery}

private class StubDisk (
    releaser: EpochReleaser
) (implicit
    random: Random,
    scheduler: Scheduler,
    disk: StubDiskDrive,
    config: StubDisk.Config
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

  def launch (checkpoints: CheckpointRegistry, pages: StubPageRegistry) {
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

  def write [P] (desc: PageDescriptor [P], obj: ObjectId, group: GroupId, page: P): Async [Position] =
    guard {
      val _page = StubPage (desc, obj, group, page)
      for {
        offset <- disk.write (_page)
      } yield {
        compactor.tally()
        Position (0, offset, align (_page.length))
      }}

  def compact (desc: PageDescriptor [_], obj: ObjectId): Async [Unit] =
    compactor.compact (desc.id, obj)

  def join [A] (task: Async [A]): Async [A] =
    releaser.join (task)
}

object StubDisk {

  class Config private (
      val checkpointProbability: Double,
      val compactionProbability: Double
  ) {

    val checkpointEntries =
      if (checkpointProbability > 0)
        (2 / checkpointProbability).toInt
      else
        Int.MaxValue

    val compactionEntries =
      if (compactionProbability > 0)
        (2 / compactionProbability).toInt
      else
        Int.MaxValue

    def checkpoint (entries: Int) (implicit random: Random): Boolean =
      checkpointProbability > 0.0 &&
        (entries > checkpointEntries || random.nextDouble < checkpointProbability)

    def compact (entries: Int) (implicit random: Random): Boolean =
      compactionProbability > 0.0 &&
        (entries > compactionEntries || random.nextDouble < compactionProbability)
  }

  object Config {

    def apply (
        checkpointProbability: Double = 0.1,
        compactionProbability: Double = 0.1
    ): StubDisk.Config = {

        require (0.0 <= checkpointProbability && checkpointProbability <= 1.0)

        require (0.0 <= compactionProbability && compactionProbability <= 1.0)

        new StubDisk.Config (
            checkpointProbability,
            compactionProbability)
    }}

  trait StubRecovery extends Recovery {

    def reattach (disk: StubDiskDrive): Async [Launch]

    def attach (disk: StubDiskDrive): Async [Launch]
  }

  def recover () (implicit
      random: Random,
      scheduler: Scheduler,
      config: StubDisk.Config
  ): StubRecovery =
    new StubRecoveryAgent () (random, scheduler, config)
}
