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

/** Recover the `SegmentLedger` from log replay, and recover the write head of the each disk's
  * `PageWriter`.
  */
private class SegmentLedgerMedic (recovery: DiskRecovery) (implicit scheduler: Scheduler) {

  /** Allocations that we've seen. */
  private var dockets = Map.empty [Long, AllocationDocket]

  /** The generation for the latest write that we've seen. */
  private var gen = -1L

  /** This position of the latest write that we've seen. */
  private var pos = Position.Null

  /** The lwrite head of each disk's `PageWriter` that we last saw. */
  private var writers = Map.empty [Int, Long]

  /** True if we've closed this medic. */
  private var closed = false

  recovery.replay (SegmentLedger.alloc) ((alloc _).tupled)
  recovery.replay (SegmentLedger.checkpoint) ((checkpoint _).tupled)

  /** Get or make the docket for the generation. */
  private def docket (gen: Long): AllocationDocket =
    if (dockets contains gen) {
      dockets (gen)
    } else {
      val d = new AllocationDocket
      dockets += gen -> d
      d
    }

  /** Replay an allocation record, if it's newer than the checkpoint. */
  def alloc (gen: Long, disk: Int, seg: Int, pos: Long, tally: PageTally): Unit =
    synchronized {
      require (!closed)
      docket (gen) .alloc (disk, seg, tally)
      writers += disk -> pos
    }

  /** Replay a checkpoint record, if it's newer than the last checkpoint. */
  def checkpoint (gen: Long, pos: Position): Unit =
    synchronized {
      require (!closed)
      if (this.gen < gen) {
        this.gen = gen
        this.pos = pos
        dockets = dockets filterKeys (gen < _)
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
    val (gen, pos, dockets) = synchronized {
      require (!closed)
      closed = true
      (this.gen, this.pos, this.dockets)
    }
    for {
      result <- readDiskTier (launch.disk, pos)
    } yield {
      // Add allocations not included in the checkpoint.
      for ((gen, docket) <- dockets; if this.gen < gen)
        result.alloc (docket)
      val maxgen = math.max (gen + 1, if (dockets.isEmpty) 0 else dockets.keys.max)
      val ledger = new SegmentLedger (launch.disk, maxgen, result)
      launch.checkpoint (ledger.checkpoint())
      launch.claim (SegmentLedger.pager, 0, Set (gen))
      launch.compact (SegmentLedger.pager) (ledger.compact _)
      (ledger, writers)
    }}}
