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
import com.treode.disk.{Compaction, Disk, DiskController, DiskEvents, DiskLaunch, ObjectId,
  PageDescriptor, PageHandler, SystemId, TypeId}

/** The second phase of building the live Disk system. Implements the user trait Launch. */
private class LaunchAgent (implicit
  scheduler: Scheduler,
  drives: DriveGroup,
  events: DiskEvents,
  val agent: DiskAgent
) extends DiskLaunch {

  private var launching = true

  implicit val disk: Disk = agent
  implicit val controller: DiskController = agent

  def launch() {
    require (launching, "Disks already launched.")
    launching = false
    agent.launch()
  }

  // The old disk system used this; this new disk system uses claim and compact instead.
  def handle (desc: PageDescriptor [_], handler: PageHandler): Unit = ()

  // TODO
  def checkpoint (f: => Async [Unit]): Unit = ???
  def claim (desc: PageDescriptor [_], obj: ObjectId, gens: Set [Long]): Unit = ???
  def compact (desc: PageDescriptor [_]) (f: Compaction => Async [Unit]) = ???
  def sysid: SystemId  = ???
}
