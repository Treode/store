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

import java.util.ArrayList

import com.treode.async.{Async, Callback, Scheduler}

private class LaunchAgent (val kit: DiskKit) extends Disk.Launch {

  private val roots = new CheckpointRegistry
  private val pages = new PageRegistry (kit)
  private var open = true

  implicit val disk: Disk = new DiskAgent (kit)

  implicit val controller: Disk.Controller = new ControllerAgent (kit, disk)

  val sysid = kit.sysid

  def requireOpen(): Unit =
    require (open, "Disk have already launched.")

  def checkpoint (f: => Async [Unit]): Unit =
    synchronized {
      requireOpen()
      roots.checkpoint (f)
    }

  def handle (desc: PageDescriptor [_], handler: PageHandler): Unit =
    synchronized {
      requireOpen()
      pages.handle (desc, handler)
    }

  def launch(): Unit =
    synchronized {
      requireOpen()
      open = false
      kit.launch (roots, pages)
    }}
