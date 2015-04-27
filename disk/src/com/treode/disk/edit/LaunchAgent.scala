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

import java.util.{ArrayList, HashMap}

import com.treode.async.{Async, Scheduler}
import com.treode.disk.{Compaction, Disk, DiskController, DiskEvents, DiskLaunch, GenerationDocket,
  ObjectId, PageDescriptor, PageHandler, SystemId, TypeId}

/** The second phase of building the live Disk system. Implements the user trait Launch. */
private class LaunchAgent (implicit
  scheduler: Scheduler,
  drives: DriveGroup,
  events: DiskEvents,
  val agent: DiskAgent
) extends DiskLaunch {

  private val checkpoints = new ArrayList [Unit => Async [Unit]]
  private val compactors = new HashMap [TypeId, Compaction => Async [Unit]]

  private val claims = new GenerationDocket
  private var ledger: SegmentLedger = null
  private var writers: Map [Int, Long] = null
  private var launching = true

  implicit val disk: Disk = agent
  implicit val controller: DiskController = agent

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
      require (!(compactors containsKey typ), s"Compactor already registered for $typ")
      compactors.put (typ, f)
    }

  def recover (ledger: SegmentLedger, writers: Map [Int, Long]): Unit =
    synchronized {
      assert (this.ledger == null && this.writers == null)
      this.ledger = ledger
      this.writers = writers
    }

  def launch() {
    require (launching, "Disks already launched.")
    launching = false
    ledger.claim (claims)
    val checkpointer = new Checkpointer (drives, checkpoints)
    val compactor = new Compactor (compactors)
    agent.launch (ledger, writers, checkpointer, compactor)
  }

  // The old disk system used this; this new disk system uses claim and compact instead.
  def handle (desc: PageDescriptor [_], handler: PageHandler): Unit = ()

  // TODO
  def sysid: SystemId  = ???
}
