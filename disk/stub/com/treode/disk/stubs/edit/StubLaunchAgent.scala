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

import scala.collection.mutable.HashMap
import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.misc.EpochReleaser
import com.treode.disk.{CheckpointerRegistry, CompactorRegistry, Compaction, DiskController,
  DiskEvents, DiskLaunch, GenerationDocket, ObjectId, PageDescriptor, PageHandler, SystemId, TypeId}
import com.treode.disk.stubs.StubDiskDrive

private class StubLaunchAgent (
  drive: StubDiskDrive,
  val disk: StubDiskAgent
) (implicit
  random: Random,
  scheduler: Scheduler,
  events: DiskEvents
) extends DiskLaunch {

  private val checkpointers = new CheckpointerRegistry.Builder
  private val compactors = new CompactorRegistry.Builder
  private val claims = new GenerationDocket
  private var open = true

  def checkpoint (f: => Async [Unit]): Unit =
    synchronized {
      require (open, "Must add checkpointer before launching.")
      checkpointers.add (f)
    }

  def claim (desc: PageDescriptor [_], obj: ObjectId, gens: Set [Long]): Unit =
    synchronized {
      require (open, "Must claim pages before launching.")
      claims.add (desc.id, obj, gens)
    }

  def compact (desc: PageDescriptor [_]) (f: Compaction => Async [Unit]): Unit =
    synchronized {
      require (open, "Must register compactors before launching.")
      compactors.add (desc) (f)
    }

  def handle (desc: PageDescriptor [_], handler: PageHandler): Unit = ???

  def launch (crashed: Boolean): Unit =
    synchronized {
      require (open, "The StubDisk has already launched.")
      open = false
      drive.claim (crashed, claims)
      disk.launch (checkpointers.result, compactors.result)
    }

  def launch(): Unit =
    launch (true)

  def controller: DiskController =
    throw new UnsupportedOperationException ("The StubDisk does not provide a controller.")

  val sysid = SystemId (0, 0)
}
