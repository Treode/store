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

import com.treode.async.{Async, Scheduler}

/** The second phase of building the live Disk system. Implements the user trait Launch. */
private class LaunchAgent (
  checkpoints: CheckpointerRegistry.Builder,
  compactors: CompactorRegistry.Builder,
  claims: GenerationDocket,
  ledger: SegmentLedger,
  writers: Map [Int, Long]
) (implicit
  scheduler: Scheduler,
  drives: DriveGroup,
  events: DiskEvents,
  val agent: DiskAgent
) extends DiskLaunch {

  private var launching = true

  implicit val disk: Disk = agent
  implicit val controller: DiskController = agent

  def checkpoint (f: => Async [Unit]): Unit =
    synchronized {
      require (launching, "Must add checkpoint before launching.")
      checkpoints.add (f)
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
      compactors.add (desc) (f)
    }

  def launch() {
    require (launching, "Disks already launched.")
    launching = false
    ledger.claim (claims)
    agent.launch (writers, checkpoints.result, compactors.result)
  }

  def sysid: SystemId =
    drives.sysid
}
