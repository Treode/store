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

import java.nio.file.{Path, Paths}
import scala.util.Random

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.io.File
import com.treode.async.io.stubs.StubFile
import com.treode.async.misc.EpochReleaser
import com.treode.disk._

import Async.{guard, supply}
import Callback.ignore

private class StubRecoveryAgent (implicit
    random: Random,
    scheduler: Scheduler,
    config: StubDiskConfig
) extends StubDiskRecovery {

  private val records = new RecordRegistry
  private var open = true

  def requireOpen(): Unit =
    require (open, "Recovery has already begun.")

  def replay [R] (desc: RecordDescriptor [R]) (f: R => Any): Unit =
    synchronized {
      requireOpen()
      records.replay (desc) (f)
    }

  def reattach (drive: StubDiskDrive): Async [DiskLaunch] =
    guard {
      synchronized {
        requireOpen()
        open = false
      }
      for {
        _ <- drive.replay (records)
      } yield {
        val releaser = new EpochReleaser
        val disk = new StubDisk (releaser) (random, scheduler, drive, config)
        new StubLaunchAgent (releaser, disk) (random, scheduler, drive, config)
      }}

  def reattach (items: Path*): Async [DiskLaunch] =
    guard (throw new UnsupportedOperationException ("The StubDisk does not use files."))
}
