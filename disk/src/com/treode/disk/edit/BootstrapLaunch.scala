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

import com.treode.async.{Async, Scheduler}
import com.treode.disk.{Compaction, Disk, DiskController, DiskEvents, DiskLaunch, GenerationDocket,
  ObjectId, PageDescriptor, PageHandler, SystemId}

private class BootstrapLaunch (implicit
  scheduler: Scheduler,
  events: DiskEvents,
  val disk: BootstrapDisk
) extends DiskLaunch {

  private val checkpoints = new Checkpoints
  private val compactors = new Compactors
  private val claims = new GenerationDocket
  private var launching = true

  def checkpoint (f: => Async [Unit]): Unit =
    synchronized {
      require (launching, "Must add checkpoint before launching.")
      checkpoints.add (_ => f)
    }

  def claim (desc: PageDescriptor [_], obj: ObjectId, gens: Set [Long]): Unit =
    synchronized {
      require (launching, "Must claim pages before launching.")
      claims.add (desc.id, obj, gens)
    }

  def compact (desc: PageDescriptor [_]) (f: Compaction => Async [Unit]): Unit =
    synchronized {
      val typ = desc.id
      require (launching, "Must add checkpoint before launching.")
      require (!(compactors containsKey typ), s"Compactor already registered for $typ")
      compactors.put (typ, f)
    }

  implicit def controller: DiskController =
    throw new IllegalStateException ("No DiskController during bootstrap.")

  def launch(): Unit =
    throw new IllegalStateException ("Cannot launch disks from BootstrapLauncher.")

  def result (ledger: SegmentLedger, writers: Map [Int, Long]): LaunchAgent = {
    val (group, agent) = disk.result (ledger)
    new LaunchAgent (checkpoints, compactors, claims, ledger, writers) (scheduler, group, events, agent)
  }

  // TODO
  def sysid: SystemId  = ???
  def handle (desc: PageDescriptor [_], handler: PageHandler): Unit = ???
}