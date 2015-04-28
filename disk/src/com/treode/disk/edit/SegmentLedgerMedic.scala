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

import com.treode.async.{Async, Scheduler}, Async.supply
import com.treode.disk.{Disk, DiskLaunch, DiskRecovery, ObjectId, Position, TypeId}

/** Recover the `SegmentLedger` from log replay, and recover the write head of the each disk's
  * `PageWriter`.
  */
private class SegmentLedgerMedic (recovery: DiskRecovery) (implicit scheduler: Scheduler) {

  /** The latest generation that we've seen. */
  private var gen = 0L

  /** The generation for the latest write that we've seen. */
  private var dgen = 0L

  /** This position of the latest write that we've seen. */
  private var pos = Position.Null

  /** Replay of allocations for the latest generation that we've seen. */
  private var primary = new AllocationDocket

  /** Replay of allocations for the second to latest generation that we've seen; emtpy if the
    * write on disk is the second to latest generation.
    */
  private var secondary = new AllocationDocket

  /** The write heads of each disk's `PageWriter`. */
  private var writers = Map.empty [Int, Long]

  /** True if we've closed this medic. */
  private var closed = false

  recovery.replay (SegmentLedger.alloc) ((alloc _).tupled)
  recovery.replay (SegmentLedger.checkpoint) ((checkpoint _).tupled)

  /** Replay an allocation record. */
  def alloc (gen: Long, disk: Int, seg: Int, pos: Long, tally: PageTally): Unit =
    synchronized {
      require (!closed)
      if (this.gen == gen) {
        // This applies to the latest generation.
        primary.alloc (disk, seg, tally)
      } else if (this.gen < gen) {
        // This applies to a new generation.
        this.gen = gen
        secondary.alloc (primary)
        primary = new AllocationDocket
        primary.alloc (disk, seg, tally)
      }
      writers += disk -> pos
    }

  /** Replay a checkpoint record. */
  def checkpoint (gen: Long, pos: Position): Unit =
    synchronized {
      require (!closed)
      if (this.gen == gen + 1) {
        // This checkpoint is for the second to latest generation.
        this.pos = pos
        this.dgen = gen
        secondary = new AllocationDocket
      } else if (this.gen < gen + 1) {
        // This checkpoint is for a newer generation.
        this.gen = gen + 1
        this.dgen = gen
        this.pos = pos
        primary = new AllocationDocket
        secondary = new AllocationDocket
      }}

  /** Read the generation from disk, if any. */
  private def readDiskTier (system: Disk, pos: Position): Async [AllocationDocket] =
    if (pos == Position.Null)
      supply (new AllocationDocket)
    else
      system.read (SegmentLedger.pager, pos)

  /** Log replay has completed; close the out the recovery, yield the recovered ledger and the
    * recovered write heads for each disk's `PageWriter`.
    */
  def close () (implicit launch: DiskLaunch): Async [(SegmentLedger, Map [Int, Long])] = {
    val (gen, pos, primary, secondary) = synchronized {
      require (!closed)
      closed = true
      (this.gen, this.pos, this.primary, this.secondary)
    }
    for {
      dtier <- readDiskTier (launch.disk, pos)
    } yield {
      primary.alloc (secondary)
      primary.alloc (dtier)
      val ledger = new SegmentLedger (launch.disk, gen, primary)
      launch.checkpoint (ledger.checkpoint())
      launch.claim (SegmentLedger.pager, 0, Set (dgen))
      launch.compact (SegmentLedger.pager) (ledger.compact _)
      (ledger, writers)
    }}}
