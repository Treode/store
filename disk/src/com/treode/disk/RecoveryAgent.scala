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

import java.io.IOException
import java.nio.file.{Path, StandardOpenOption}, StandardOpenOption._

import com.treode.async.{Async, AsyncQueue, BatchIterator, Callback, Fiber, Scheduler}, Async.supply
import com.treode.async.implicits._
import com.treode.async.misc.EpochReleaser
import com.treode.disk.exceptions.ReattachException

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
  private var opening = Set.empty [Path]

  /** The paths we have opened; we have their superblocks and drives. */
  private var opened = Set.empty [Path]

  /** Failures encountered while trying to open any paths. */
  private var failures = Seq.empty [Throwable]

  /** Readers that we have successfully opened. */
  private var readers = Seq.empty [LogReader]

  /** The latest superblock we've read from a drive so far. */
  private var common = SuperBlock.Common.empty

  /** The callback for when recovery completes. */
  private var recovery = Option.empty [Callback [DiskLaunch]]

  /** The record registry we are building before beginning replay. */
  private val records = new RecordRegistry

  /** The collector for this recovery. */
  private val collector = new LogCollector

  private val ledger = new SegmentLedgerMedic (this)

  private def reengage() {
    if (!reattachments.isEmpty)
      _reattach()
    else if (opening.isEmpty && !recovery.isEmpty)
      _recover()
  }

  /** We successfully opened a path, read its superblock and made a reader. */
  private def _reattach (path: Path, common: SuperBlock.Common, reader: LogReader): Unit =
    fiber.execute {
      opening -= path
      opened += path
      reattachments ++= (common.paths -- opened)
      readers +:= reader
      if (this.common.gen < common.gen) this.common = common
      queue.engage()
    }

  /** We failed to open a path and read its superblock. */
  private def _reattach (path: Path, thrown: Throwable): Unit =
    fiber.execute {
      opening -= path
      failures +:= thrown
      queue.engage()
    }

  /** Open the next queued path, read its superblock and make a reader. */
  private def _reattach() {
    val path = reattachments.head
    reattachments = reattachments.tail
    opening += path
    queue.begin {
      try {
        val file = files.open (path, READ, WRITE)
        SuperBlock.read (file)
        .map { superb =>
          val reader = new LogReader (path, file, superb, records, collector)
          _reattach (path, superb.common, reader)
        }
        .recover { case thrown =>
          file.close()
          _reattach (path, thrown)
        }
      } catch {
        case thrown: IOException =>
          _reattach (path, thrown)
          supply (())
      }}}

  /** Finish the recovery phase. */
  private def _recover() {
    val cb = recovery.get
    recovery = null
    if (failures.isEmpty) {
      val (attached, detached) = readers.partition (r => common.paths contains r.path)
      events.reattachingDisks (
        reattaching = attached.map (_.path) .toSet,
        detached = detached.map (_.path) .toSet)
      for (r <- detached)
        r.close()
      (for {
        // Merge the entries from all readers (drives), sorted by batch number.
        _ <- BatchIterator.merge (attached) .batch (RecoveryAgent.replay _)
        // The collector aggregated results from all readers. Now make the bootstrap system.
        boot = collector.result (common)
        // Use the bootstrap system to recover the SegmentLedger.
        (ledger, writers) <- this.ledger.close () (boot)
      } yield {
        // Use the SegmentLedger to construct the full disk system.
        val launch = boot.result (ledger, writers)
        // Replace the bootstrap system with the full one in the SegmentLedger.
        ledger.booted (launch.disk)
        launch
      }) .run (cb on scheduler)
    } else {
      for (r <- readers)
        r.close()
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
    }

  def init (sysid: SystemId): Async [DiskLaunch] =
    fiber.async { cb =>
      requireNotStarted ("Must init disks without starting recovery.")
      common = common.copy (sysid = sysid)
      recovery = Some (cb)
      queue.engage()
    }}

private object RecoveryAgent {

  def replay (xss: Iterable [LogEntries]): Async [Unit] =
    supply {
      for (xs <- xss) {
        for (x <- xs.entries)
          x (())
      }}}
