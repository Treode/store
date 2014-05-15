package com.treode.store.tier

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.treode.async.{Async, AsyncIterator, Callback, Scheduler}
import com.treode.disk.{Disks, PageDescriptor, Position}
import com.treode.store._

import Async.{async, guard, supply, when}
import Callback.ignore
import SynthTable.{genStepSize, genStepMask}
import TierTable.Meta

private class SynthTable (

    val desc: TierDescriptor,

    val id: TableId,

    // To lock the generation and references to the primary and secondary; this locks the references
    // only, while the skip list manages concurrent readers and writers of entries.  Writing to the
    // table requires only a read lock on the references to ensure that the compactor does not change
    // them. The compactor uses a write lock to move the primary to secondary, allocate a new
    // primary, and increment the generation.
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
    disks: Disks,
    config: StoreConfig
) extends TierTable {
  import desc.pager
  import scheduler.whilst

  val readLock = lock.readLock()
  val writeLock = lock.writeLock()

  var writting = Set.empty [Long]

  def typ = desc.id

  def get (key: Bytes, time: TxClock): Async [Cell] = guard {

    val mkey = Key (key, time)

    readLock.lock()
    val (primary, secondary, tiers) = try {
      (this.primary, this.secondary, this.tiers)
    } finally {
      readLock.unlock()
    }

    var candidate = Cell.sentinel
    var entry = primary.ceilingEntry (mkey)
    if (entry != null) {
      val Key (k, t) = entry.getKey
      if (key == k && candidate.time < t) {
        candidate = memTierEntryToCell (entry)
      }}

    entry = secondary.ceilingEntry (mkey)
    if (entry != null) {
      val Key (k, t) = entry.getKey
      if (key == k && candidate.time < t) {
        candidate = memTierEntryToCell (entry)
      }}

    var i = 0
    whilst (i < tiers.size) {
      val tier = tiers (i)
      i += 1
      when (tier.earliest <= time && candidate.time < tier.latest) {
        tier.get (desc, key, time) .map {
         case Some (c @ Cell (k, t, v)) if key == k && candidate.time < t =>
            candidate = c
          case _ =>
            ()
        }}
    } .map { _ =>
      if (candidate == Cell.sentinel)
        Cell (key, TxClock.zero, None)
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

  def iterator (key: Bytes, time: TxClock, residents: Residents): CellIterator = {
    readLock.lock()
    val (primary, secondary, tiers) = try {
      (this.primary, this.secondary, this.tiers)
    } finally {
      readLock.unlock()
    }
    TierIterator
        .merge (desc, key, time, primary, secondary, tiers)
        .clean (desc, id, residents)
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

  def probe (groups: Set [Long]): Set [Long] = {
    readLock.lock()
    val (gen, active, writting) = try {
      (this.gen, tiers.active, this.writting)
    } finally {
      readLock.unlock()
    }
    active ++ writting
  }

  def compact(): Unit =
    pager.compact (id.id) run (ignore)

  def compact (groups: Set [Long], residents: Residents): Async [Meta] = guard {

    writeLock.lock()
    val (chosen, gen) = try {
      val c = tiers.choose (groups, residents)
      val g = tiers.gen + 1
      assert ((g & genStepMask) != 0, "Tier compacted too many times")
      writting += g
      (c, g)
    } finally {
      writeLock.unlock()
    }

    val iter = TierIterator .merge (desc, chosen) .clean (desc, id, residents)
    val est = countMemTierKeys (primary) + chosen.keys
    for {
      tier <- TierBuilder.build (desc, id, gen, est, residents, iter)
    } yield {
      writeLock.lock()
      try {
        tiers = tiers.compacted (tier, chosen)
        writting -= gen
        new Meta (tiers.gen, tiers)
      } finally {
        writeLock.unlock()
      }}}

  def checkpoint (residents: Residents): Async [Meta] = guard {

    writeLock.lock()
    val (gen, primary) = try {
      require (secondary.isEmpty, "Checkpoint already in progress.")
      val g = this.gen
      val p = this.primary
      this.gen += genStepSize
      this.primary = secondary
      secondary = p
      writting += g
      (g, p)
    } finally {
      writeLock.unlock()
    }

    val iter = TierIterator .adapt (primary) .clean (desc, id, residents)
    val est = countMemTierKeys (primary)
    for {
      tier <- TierBuilder.build (desc, id, gen, est, residents, iter)
    } yield {
      writeLock.lock()
      try {
        secondary = newMemTier
        tiers = tiers.compacted (tier, Tiers.empty)
        writting -= gen
        new Meta (gen, tiers)
      } finally {
        writeLock.unlock()
      }}}}

private object SynthTable {

  val genStepSize = 128
  val genStepMask = genStepSize - 1

  def apply (desc: TierDescriptor, id: TableId) (
      implicit scheduler: Scheduler, disk: Disks, config: StoreConfig): SynthTable = {
    val lock = new ReentrantReadWriteLock
    new SynthTable (desc, id, lock, 0, newMemTier, newMemTier, Tiers.empty)
  }}
