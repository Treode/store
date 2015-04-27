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

import java.nio.file.{Path, StandardOpenOption}, StandardOpenOption._

import com.treode.async.{Async, AsyncQueue, BatchIterator, Callback, Fiber, Scheduler}
import com.treode.async.implicits._
import com.treode.disk.{Disk, DiskRecovery, DiskConfig, DiskEvents, DiskLaunch, FileSystem,
  RecordDescriptor, RecordRegistry}

/** The first phase of building the live Disk system. Implements the user trait Recovery.
  *
  * When opening a drive, we may discover new drives to open. This queues paths that need to be
  * opened.
  */
private class RecoveryAgent (implicit
  files: FileSystem,
  scheduler: Scheduler,
  config: DiskConfig,
  events: DiskEvents
) extends DiskRecovery {

  private val fiber = new Fiber
  private val queue = new AsyncQueue (fiber) (reengage _)

  /** The paths queued to be opened; we need to read their superblocks. */
  private var reattachments = Set.empty [Path]

  /** The paths being opened; we are awaiting their superblocks and drives. */
  private var reattaching = Set.empty [Path]

  /** The paths we have opened; we have their superblocks and drives. */
  private var reattached = Set.empty [Path]

  /** Failures encountered while trying to open any paths. */
  private var failures = Seq.empty [ReattachFailure]

  /** Readers that we have successfully opened. */
  private var readers = Seq.empty [LogReader]

  /** The latest superblock we've read from a drive so far. */
  private var common = SuperBlock.Common.empty

  /** The callback for when recovery completes. */
  private var recovery = Option.empty [Callback [DiskLaunch]]

  /** The record registry we are building before beginning replay. */
  private val records = new RecordRegistry

  /** The replayer for this recovery. */
  private val replayer = new LogReplayer

  /** The LogDispatcher for the final system. */
  private val logdsp = new LogDispatcher

  /** The PageDispatcher for the final system. */
  private val pagdsp = new PageDispatcher

  private val ledger = new SegmentLedgerMedic (this)

  queue.launch()

  private def reengage() {
    if (!reattachments.isEmpty)
      _reattach()
    else if (reattaching.isEmpty && !recovery.isEmpty)
      _recover()
  }

  /** We successfully opened a path, read its superblock and made a reader. */
  private def _reattach (path: Path, common: SuperBlock.Common, reader: LogReader): Unit =
    fiber.execute {
      reattaching -= path
      reattached += path
      reattachments ++= (common.paths -- reattached)
      readers +:= reader
      if (this.common.gen < common.gen) this.common = common
      queue.engage()
    }

  /** We failed to open a path and read its superblock. */
  private def _reattach (path: Path, thrown: Throwable): Unit =
    fiber.execute {
      reattaching -= path
      failures +:= ReattachFailure (path, thrown)
      queue.engage()
    }

  /** Open the next queued path, read its superblock and make a reader. */
  private def _reattach() {
    val path = reattachments.head
    reattachments = reattachments.tail
    reattaching += path
    queue.begin {
      val file = files.open (path, READ, WRITE)
      SuperBlock.read (file)
      .map { superb =>
        val reader = new LogReader (path, file, superb, records, replayer, logdsp, pagdsp)
        _reattach (path, superb.common, reader)
      }
      .recover { case thrown =>
        file.close()
        _reattach (path, thrown)
      }}}

  /** Finish the recovery phase. */
  private def _recover() {
    val cb = recovery.get
    recovery = null
    if (failures.isEmpty) {
      events.reattachingDisks (reattached)
      (for {
        _ <- BatchIterator.merge (readers) .batch (replayer.replay _)
        launch = {
          logdsp.batch = replayer.batch
          val drives = new DriveGroup (logdsp, pagdsp, replayer.drives, common.gen, common.dno)
          val agent = new DiskAgent (logdsp, pagdsp, drives)
          new LaunchAgent () (scheduler, drives, events, agent)
        }
        (ledger, writers) <- this.ledger.close () (launch)
      } yield {
        launch.recover (ledger, writers)
        launch
      }) .run (cb on scheduler)
    } else {
      scheduler.fail (cb, new ReattachException (failures))
    }}

  private def requireNotStarted (message: String): Unit =
    require (recovery != null && recovery.isEmpty, message)

  def replay [R] (desc: RecordDescriptor [R]) (f: R => Any): Unit =
    fiber.execute {
      requireNotStarted ("Must register replayers before starting recovery.")
      records.replay (desc) (f)
    }

  def reattach (paths: Path*): Async [DiskLaunch] =
    fiber.async { cb =>
      requireNotStarted ("Must reattach disks before starting recovery.")
      reattachments ++= paths
      recovery = Some (cb)
      queue.engage()
    }}
