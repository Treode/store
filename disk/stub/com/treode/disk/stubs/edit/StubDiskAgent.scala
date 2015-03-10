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

import scala.collection.mutable.UnrolledBuffer
import scala.util.{Failure, Random, Success}

import com.treode.async.{Async, Callback, Scheduler}, Async.{async, guard}, Callback.fanout
import com.treode.async.misc.EpochReleaser
import com.treode.disk.{CheckpointerRegistry, CompactorRegistry, Dispatcher, ObjectId,
  PageDescriptor, Position, RecordDescriptor}
import com.treode.disk.stubs.{StubDiskDrive, StubPage, StubRecord}

private class StubDiskAgent (
  drive: StubDiskDrive
) (implicit
  random: Random,
  scheduler: Scheduler
) extends StubDisk {

  val logdsp = new Dispatcher [(StubRecord, Callback [Unit])]
  val releaser = new EpochReleaser
  val checkpointer = new StubCheckpointer (drive)
  val compactor = new StubCompactor

  logdsp.receive (receiver _)

  def align (n: Int): Int = {
    val bits = 6
    val mask = (1 << bits) - 1
    (n + mask) & ~mask
  }

  def receiver (batch: Long, records: UnrolledBuffer [(StubRecord, Callback [Unit])]) {
    logdsp.send (new UnrolledBuffer [(StubRecord, Callback [Unit])])
    val cb = fanout (records .map (_._2))
    drive.log (records .map (_._1) .toSeq) .run { v =>
      logdsp.receive (receiver _)
      cb (v)
    }}

  def launch (checkpointers: CheckpointerRegistry, compactors: CompactorRegistry) {
    checkpointer.launch (checkpointers)
    compactor.launch (compactors)
  }

  def record [R] (desc: RecordDescriptor [R], entry: R): Async [Unit] =
    async { cb =>
      logdsp.send ((StubRecord (desc, entry), cb))
    }

  def read [P] (desc: PageDescriptor [P], pos: Position): Async [P] =
    guard {
      for {
        page <- drive read (pos.offset) on scheduler
      } yield {
        require (align (page.length) == pos.length)
        desc.ppag.fromByteArray (page.data)
      }}

  def write [P] (desc: PageDescriptor [P], obj: ObjectId, gen: Long, page: P): Async [Position] =
    guard {
      val _page = StubPage (desc, obj, gen, page)
      for {
        offset <- drive write (_page) on scheduler
      } yield {
        Position (0, offset, align (_page.length))
      }}

  def compact (desc: PageDescriptor [_], obj: ObjectId): Unit =
    compactor.compact (desc.id, obj)

  def release (desc: PageDescriptor [_], obj: ObjectId, gens: Set [Long]): Unit =
    releaser.release (drive.release (desc.id, obj, gens))

  def join [A] (task: Async [A]): Async [A] =
    releaser.join (task)

  def checkpoint(): Async [Unit] =
    checkpointer.checkpoint()

  def drain(): Unit =
    compactor.compact (drive.drainable)
}
