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

import com.treode.async.{Async, Scheduler}, Async.supply

/** Aggregates results from LogReaders of individual disks. */
private class LogCollector (implicit
  files: FileSystem,
  scheduler: Scheduler,
  config: DiskConfig,
  events: DiskEvents
) {

  /** All the drives collected. */
  private var drives = Map.empty [Int, BootstrapDrive]

  /** The largest batch number seen. */
  private var batch = 0L

  /** The total bytes of log entries replayed. */
  private var bytes = 0L

  /** The total count of log entries replayed. */
  private var entries = 0L

  /** Close replay of a drive.
    *
    * @param drive The drive, with its LogWriter ready to append where reading finished.
    * @param batch The last batch replayed from the log this drive.
    * @param bytes The count of bytes replayed from the log on this drive.
    * @param entries The count of entries replayed from the log on this drive.
    */
  def reattach (drive: BootstrapDrive, batch: Long, bytes: Long, entries: Long): Unit =
    synchronized {
      drives += drive.id -> drive
      this.batch = math.max (this.batch, batch)
      this.bytes += bytes
      this.entries += entries
    }

  def result (common: SuperBlock.Common): BootstrapLaunch = {
    implicit val disk = new BootstrapDisk (common, drives, batch, bytes, entries)
    new BootstrapLaunch
  }}
