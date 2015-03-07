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

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.misc.EpochReleaser
import com.treode.disk._

import Disk.{Controller, Launch}

private class StubLaunchAgent (
    releaser: EpochReleaser,
    val disk: StubDisk
) (implicit
    random: Random,
    scheduler: Scheduler,
    drive: StubDiskDrive,
    config: StubDisk.Config
) extends Launch {

  private val roots = new CheckpointRegistry
  private val pages = new StubPageRegistry (releaser)
  private var open = true

  def requireOpen(): Unit =
    require (open, "Disk have already launched.")

  def checkpoint (f: => Async [Unit]): Unit =
    synchronized {
      requireOpen()
      roots.checkpoint (f)
    }

  def handle (desc: PageDescriptor [_], handler: PageHandler): Unit =
    pages.handle (desc, handler)

  def launch(): Unit =
    synchronized {
      requireOpen()
      open = false
      disk.launch (roots, pages)
    }

  def controller: Controller =
    throw new UnsupportedOperationException ("The StubDisk do not provide a controller.")

  val sysid = new Array [Byte] (0)
}
