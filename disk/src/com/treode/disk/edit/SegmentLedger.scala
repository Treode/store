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

import com.treode.async.{Async, Callback, Scheduler}, Async.{async, supply}, Callback.ignore
import com.treode.disk.{Compaction, Disk, GenerationDocket, ObjectId, PageDescriptor, PickledPage,
  Position, RecordDescriptor, TypeId}

/** Mediates access to the global `AllocationDocket`, and logs and checkpoints it on disk. */
private class SegmentLedger (
  system: Disk,
  private var gen: Long,
  private var mem: AllocationDocket
) (implicit
  scheduler: Scheduler
) {

  def this (system: Disk) (implicit scheduler: Scheduler) =
    this (system, 0, new AllocationDocket)

  /** List of requests to write. */
  private var writes = List.empty [Callback [Unit]]

  /** Is a write currently in flight? */
  private var writing = false

  /** Remove unclaimed generations; launch invokes this to clean up after a crash. */
  def claim (claims: GenerationDocket) {
    claims.add (SegmentLedger.pager.id, 0, gen)
    synchronized (mem.claim (claims))
  }

  /** Add to the allocations, and log the write head for the disk's `PageWriter`. */
  def alloc (disk: Int, seg: Int, pos: Long, tally: PageTally): Async [Unit] = {
    val gen = synchronized {
      mem.alloc (disk, seg, tally)
      this.gen
    }
    system.record (SegmentLedger.alloc, (gen, disk, seg, pos, tally))
  }

  /** Subtract the allocations for the object's generations. */
  def free (typ: TypeId, obj: ObjectId, gens: Set [Long]): SegmentDocket =
    synchronized (mem.free (typ, obj, gens))

  /** A copy of the current allocations. */
  def docket: AllocationDocket =
    synchronized (this.mem.clone)

  /** Write the ledger to disk, ensures only one write is in flight at a time. */
  private def write() {
    val (gen, mem, writes) =
      synchronized {
        if (writing || this.writes.isEmpty) return
        writing = true
        val gen = this.gen
        val mem = this.mem.clone
        val writes = this.writes
        this.gen += 1
        this.writes = List.empty
        (gen, mem, writes)
      }
    (for {
      pos <- system.write (SegmentLedger.pager, 0, gen, mem)
      _ <- system.record (SegmentLedger.checkpoint, (gen, pos))
    } yield {
      system.release (SegmentLedger.pager, 0, Set (gen-1))
      for (cb <- writes) scheduler.pass (cb, ())
      synchronized (writing = false)
      write()
    }) run (ignore)
  }

  private def write (cb: Callback [Unit]) {
    synchronized (writes ::= cb)
    write()
  }

  def checkpoint(): Async [Unit] =
    async (write _)

  def compact (compaction: Compaction): Async [Unit] =
    synchronized {
      if (compaction.gens contains (gen - 1))
        async (write _)
      else
        supply (())
    }}

private object SegmentLedger {

  val alloc = {
    import DiskPicklers._
    RecordDescriptor (0xE32307CD55B84D22L, tuple (ulong, uint, uint, ulong, pageTally))
  }

  val checkpoint = {
    import DiskPicklers._
    RecordDescriptor (0x2AC569000C437A49L, tuple (ulong, position))
  }

  val pager = {
    import DiskPicklers._
    PageDescriptor (0x303C39F2A9DC8FC8L, allocationDocket)
  }}
