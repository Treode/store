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

package com.treode.store.tier

import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.JavaConversions

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.implicits._
import com.treode.disk.{Disk, GroupId, PageDescriptor, Position}
import com.treode.store._

import Async.{async, guard, supply, when}
import Callback.ignore
import JavaConversions._
import Scheduler.toRunnable
import SynthTable.{genStepMask, genStepSize}
import TierTable.{Checkpoint, Compaction}

private class SynthTable (

    val desc: TierDescriptor,

    val id: TableId,

    // To lock the generation and references to the primary and secondary; this locks the
    // references only, while the skip list manages concurrent readers and writers of entries.
    // Writing to the table requires only a read lock on the references to ensure that the
    // checkpointer and compactor do not change them. The checkpointer uses a write lock to move
    // the primary to secondary, allocate a new primary, and increment the generation. The
    // compactor uses the write lock to replace multiple tiers with a compacted one.
    lock: ReentrantReadWriteLock,

    var gen: Long,

    // This resides in memory and it is the only tier that receives puts.
    var primary: MemTier,

    // This tier resides in memory and is being written to disk.
    var secondary: MemTier,

    // The position of each tier written on disk.
    var tiers: Tiers

) (implicit
    scheduler: Scheduler,
    disk: Disk,
    config: Store.Config
) extends TierTable {
  import desc.pager
  import scheduler.whilst

  val readLock = lock.readLock()
  val writeLock = lock.writeLock()
  var queue = List.empty [Runnable]

  def typ = desc.id

  def get (key: Bytes, time: TxClock): Async [Cell] = disk.join {

    val mkey = Key (key, time)

    readLock.lock()
    val (primary, secondary, tiers) = try {
      (this.primary, this.secondary, this.tiers)
    } finally {
      readLock.unlock()
    }

    var candidate: Cell = null

    // Check the primary memtable for an entry.
    var entry = primary.ceilingEntry (mkey)
    if (entry != null) {
      val Key (k, t) = entry.getKey
      // Use this entry if we do not have a candidate, or if this entry is newer than the
      // candidate. Note, the skip list already ensured that this entry is older than `time`.
      if (key == k && (candidate == null || candidate.time < t)) {
        candidate = memTierEntryToCell (entry)
      }}

    // Check the secondary memtable for an entry.
    entry = secondary.ceilingEntry (mkey)
    if (entry != null) {
      val Key (k, t) = entry.getKey
      if (key == k && (candidate == null || candidate.time < t)) {
        candidate = memTierEntryToCell (entry)
      }}

    // Check the disk tiers for entries.
    var i = 0
    whilst (i < tiers.size) {
      val tier = tiers (i)
      i += 1
      // Check this tier only if it we have no candidate, or if the tier has entries newer than
      // the candidate.
      when (tier.earliest <= time && (candidate == null || candidate.time < tier.latest)) {
        tier.get (desc, key, time) .map {
         case Some (c @ Cell (k, t, v)) if key == k && (candidate == null || candidate.time < t) =>
            candidate = c
          case _ =>
            ()
        }}
    } .map { _ =>
      if (candidate == null)
        Cell (key, TxClock.MinValue, None)
      else
        candidate
    }}

  def put (key: Bytes, time: TxClock, value: Bytes): Long = {
    readLock.lock()
    try {
      primary.put (Key (key, time), Some (value))
      gen
    } finally {
      readLock.unlock()
    }}

  def delete (key: Bytes, time: TxClock): Long = {
    readLock.lock()
    try {
      primary.put (Key (key, time), None)
      gen
    } finally {
      readLock.unlock()
    }}

  def iterator (residents: Residents): CellIterator = {
    readLock.lock()
    val (primary, secondary, tiers) = try {
      (this.primary, this.secondary, this.tiers)
    } finally {
      readLock.unlock()
    }
    TierIterator
        .merge (desc, primary, secondary, tiers)
        .clean (desc, id, residents)
  }

  def iterator (start: Bound [Key], residents: Residents): CellIterator = {
    readLock.lock()
    val (primary, secondary, tiers) = try {
      (this.primary, this.secondary, this.tiers)
    } finally {
      readLock.unlock()
    }
    TierIterator
        .merge (desc, start, primary, secondary, tiers)
        .clean (desc, id, residents)
  }

  def iterator (start: Bound [Key], window: Window, slice: Slice, residents: Residents): CellIterator = {
    readLock.lock()
    val (primary, secondary, tiers) = try {
      (this.primary, this.secondary, this.tiers)
    } finally {
      readLock.unlock()
    }
    TierIterator
        .merge (desc, start, primary, secondary, tiers.overlaps (window))
        .clean (desc, id, residents)
        .slice (id, slice)
        .window (window)
  }

  def receive (cells: Seq [Cell]): (Long, Seq [Cell]) = {
    readLock.lock()
    try {
      val novel = Seq.newBuilder [Cell]
      for {
        cell <- cells
        key = Key (cell.key, cell.time)
        if secondary.get (key) == null
        if primary.put (key, cell.value) == null
      } novel += cell
      (gen, novel.result)
    } finally {
      readLock.unlock()
    }}

  def probe (groups: Set [GroupId]): Async [Set [GroupId]] = async { cb =>
    writeLock.lock()
    try {
      if (!secondary.isEmpty)
        queue ::= toRunnable (probe (groups), cb)
      else
        cb.on (scheduler) .pass (tiers.active map (GroupId (_)))
    } finally {
      writeLock.unlock()
    }}

  def compact(): Unit =
    pager.compact (id.id) run (ignore)

  def countKeys (tier: MemTier): Long = {
    var count = 0L
    var key: Bytes = null
    for (k <- tier.keySet; if k.key != k) {
      key = k.key
      count += 1
    }
    count
  }

  def compact (gen: Long, chosen: Tiers, residents: Residents): Async [Option [Compaction]] = guard {

    // Write the new tier. When the write has completed, update the tiers and return the new tier.
    val iter = TierIterator .merge (desc, chosen) .clean (desc, id, residents)
    val est = countKeys (primary) + chosen.estimate (residents)
    for {
      tier <- TierBuilder.build (desc, id, gen, est, residents, iter)
    } yield {
      writeLock.lock()
      val meta = try {
        val g = chosen.minGen
        tiers = tiers.compact (g, tier)
        new Some (new Compaction (g, tier))
      } finally {
        writeLock.unlock()
      }
      meta
    }}

  def compact (groups: Set [GroupId], residents: Residents): Async [Option [Compaction]] = async { cb =>

    // Choose which tiers to compact, accounting for which groups the disk cleaner needs moved,
    // and accounting for tier sizes. There may be no work to do.
    writeLock.lock()
    try {
      if (!secondary.isEmpty) {
        queue ::= toRunnable (compact (groups, residents), cb)
      } else {
        val chosen = tiers.choose (groups map (_.id), residents)
        if (chosen.isEmpty) {
          cb.on (scheduler) .pass (None)
        } else if (primary.isEmpty) {
          val gen = this.gen
          this.gen += genStepSize
          compact (gen, chosen, residents) run (cb)
        } else {
          val gen = tiers.maxGen + 1
          assert ((gen & genStepMask) != 0, "Tier compacted too many times")
          compact (gen, chosen, residents) run (cb)
        }}
    } finally {
      writeLock.unlock()
    }}

  def checkpoint (gen: Long, secondary: MemTier, residents: Residents): Async [Checkpoint] = guard {

    // Write the new tier. When the write has completed, update the set of tiers and return them.
    val iter = TierIterator .adapt (secondary) .clean (desc, id, residents)
    val est = countKeys (secondary)
    for {
      tier <- TierBuilder.build (desc, id, gen, est, residents, iter)
    } yield {
      writeLock.lock()
      val (meta, queue) = try {
        val _queue = this.queue
        this.queue = List.empty
        this.secondary = newMemTier
        tiers = tiers.checkpoint (tier)
        (new Checkpoint (gen, tiers), _queue)
      } finally {
        writeLock.unlock()
      }
      queue foreach (_.run)
      compact()
      meta
    }}

  def checkpoint (residents: Residents): Async [Checkpoint] = guard {

    // Move the primary memtable to the secondary and clear the primary. New entries on this table
    // will be written to the primary, while the TierBuilder iterates the secondary. Increase the
    // generation. This identifies tiers for probe and compact, and it tags log entries so that the
    // medic can skip old ones during replay.
    writeLock.lock()
    val (gen, secondary, tiers) = try {
      assert (this.secondary.isEmpty, "Checkpoint already in progress.")
      val _gen = this.gen
      val _secondary = this.primary
      if (!this.primary.isEmpty) {
        this.gen += genStepSize
        this.primary = this.secondary
        this.secondary = _secondary
      }
      (_gen, _secondary, this.tiers)
    } finally {
      writeLock.unlock()
    }

    // If the secondary is empty, it means nothing new was written to this table since the last
    // checkpoint. We can immediately return the current set of tiers. Otherwise, we need to
    // write a new tier, and return the new set of tiers later.
    if (secondary.isEmpty) {
      supply (new Checkpoint (gen, tiers))
    } else {
      checkpoint (gen, secondary, residents)
    }}

  def digest: TableDigest =
    new TableDigest (id, tiers.digest)
}

private object SynthTable {

  val genStepBits = 7
  val genStepSize = (1 << genStepBits).toLong
  val genStepMask = genStepSize - 1

  def apply (desc: TierDescriptor, id: TableId) (
      implicit scheduler: Scheduler, disk: Disk, config: Store.Config): SynthTable = {
    val lock = new ReentrantReadWriteLock
    new SynthTable (desc, id, lock, genStepSize, newMemTier, newMemTier, Tiers.empty)
  }}
