package com.treode.store.tier

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.treode.async.{Async, AsyncIterator, Callback, Scheduler}
import com.treode.disk.{Disks, ObjectId, PageDescriptor, Position}
import com.treode.store.{Bytes, Cell, CellIterator, Residents, StoreConfig, TxClock}

import Async.{async, supply, when}
import Callback.ignore
import TierTable.Meta

private class SynthTable (

    val desc: TierDescriptor,

    val obj: ObjectId,

    // To lock the generation and references to the primary and secondary; this locks the references
    // only, while the skip list manages concurrent readers and writers of entries.  Writing to the
    // table requires only a read lock on the references to ensure that the compactor does not change
    // them. The compactor uses a write lock to move the primary to secondary, allocate a new
    // primary, and increment the generation.
    lock: ReentrantReadWriteLock,

    var gen: Long,

    // This resides in memory and it is the only tier that is written.
    var primary: MemTier,

    // This tier resides in memory and is being compacted and written to disk.
    var secondary: MemTier,

    // The position of each tier on disk.
    var tiers: Tiers

) (implicit
    scheduler: Scheduler,
    disks: Disks,
    config: StoreConfig
) extends TierTable {
  import config.priorValueEpoch
  import desc.pager
  import scheduler.whilst

  private val readLock = lock.readLock()
  private val writeLock = lock.writeLock()

  def typ = desc.id

  def get (key: Bytes, time: TxClock): Async [Cell] = {

    val mkey = MemKey (key, time)

    readLock.lock()
    val (primary, secondary, tiers) = try {
      (this.primary, this.secondary, this.tiers)
    } finally {
      readLock.unlock()
    }

    var candidate = Cell.sentinel
    var entry = primary.ceilingEntry (mkey)
    if (entry != null) {
      val MemKey (k, t) = entry.getKey
      if (key == k && candidate.time < t) {
        candidate = memTierEntryToCell (entry)
      }}

    entry = secondary.ceilingEntry (mkey)
    if (entry != null) {
      val MemKey (k, t) = entry.getKey
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
      primary.put (MemKey (key, time), Some (value))
      gen
    } finally {
      readLock.unlock()
    }}

  def delete (key: Bytes, time: TxClock): Long = {
    readLock.lock()
    try {
      primary.put (MemKey (key, time), None)
      gen
    } finally {
      readLock.unlock()
    }}

  def iterator: CellIterator = {
    readLock.lock()
    val (primary, secondary, tiers) = try {
      (this.primary, this.secondary, this.tiers)
    } finally {
      readLock.unlock()
    }
    TierIterator
        .merge (desc, primary, secondary, tiers)
        .dedupe
        .retire (priorValueEpoch.limit)
  }

  def iterator (key: Bytes, time: TxClock): CellIterator = {
    readLock.lock()
    val (primary, secondary, tiers) = try {
      (this.primary, this.secondary, this.tiers)
    } finally {
      readLock.unlock()
    }
    TierIterator
        .merge (desc, key, time, primary, secondary, tiers)
        .dedupe
        .retire (priorValueEpoch.limit)
  }

  def receive (cells: Seq [Cell]): (Long, Seq [Cell]) = {
    readLock.lock()
    try {
      val novel = Seq.newBuilder [Cell]
      for {
        cell <- cells
        key = MemKey (cell.key, cell.time)
        if secondary.get (key) == null
        if primary.put (key, cell.value) == null
      } novel += cell
      (gen, novel.result)
    } finally {
      readLock.unlock()
    }}

  def probe (groups: Set [Long]): Set [Long] = {
    readLock.lock()
    val (tiers) = try {
      this.tiers
    } finally {
      readLock.unlock()
    }
    tiers.active
  }

  def compact(): Unit =
    pager.compact (obj) run (ignore)

  def compact (groups: Set [Long], residents: Residents) (p: Cell => Boolean): Async [Meta] =
    checkpoint (residents) (p)

  def checkpoint (residents: Residents) (p: Cell => Boolean): Async [Meta] = disks.join {

    writeLock.lock()
    val (gen, primary, tiers) = try {
      require (secondary.isEmpty)
      val g = this.gen
      val p = this.primary
      this.gen += 1
      this.primary = secondary
      this.secondary = p
      (g, p, this.tiers)
    } finally {
      writeLock.unlock()
    }

    val iter = TierIterator.merge (desc, primary, emptyMemTier, tiers) .dedupe
    val est = countMemTierKeys (primary) + tiers.keys
    for {
      tier <- TierBuilder.build (desc, obj, gen, est, residents, iter)
    } yield {
      val tiers = Tiers (tier)
      val meta = new Meta (gen, tiers)
      writeLock.lock()
      try {
        this.secondary = newMemTier
        this.tiers = tiers
      } finally {
        writeLock.unlock()
      }
      meta
    }}}

private object SynthTable {

  def apply (desc: TierDescriptor, obj: ObjectId) (
      implicit scheduler: Scheduler, disk: Disks, config: StoreConfig): SynthTable = {
    val lock = new ReentrantReadWriteLock
    new SynthTable (desc, obj, lock, 0, newMemTier, newMemTier, Tiers.empty)
  }}
