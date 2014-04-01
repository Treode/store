package com.treode.store.tier

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.treode.async.{Async, AsyncIterator, Callback, Scheduler}
import com.treode.disk.{Disks, ObjectId, PageHandler, PageDescriptor, Position}
import com.treode.store.{Bytes, StoreConfig}

import Async.{async, supply}
import TierTable.Meta

private class SynthTable [K, V] (

    val desc: TierDescriptor [K, V],

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

) (
    implicit scheduler: Scheduler,
    disks: Disks,
    config: StoreConfig
) extends TierTable {
  import scheduler.whilst

  private val readLock = lock.readLock()
  private val writeLock = lock.writeLock()

  private def read (key: Bytes, limit: Bytes): Async [TierCell] = {

    readLock.lock()
    val (primary, secondary, tiers) = try {
      (this.primary, this.secondary, this.tiers)
    } finally {
      readLock.unlock()
    }

    var entry = primary.ceilingEntry (key)
    if (entry != null && entry.getKey <= limit)
      return supply (TierCell (entry.getKey, entry.getValue))

    entry = secondary.ceilingEntry (key)
    if (entry != null && entry.getKey <= limit)
      return supply (TierCell (entry.getKey, entry.getValue))

    var cell = Option.empty [TierCell]
    var i = 0
    for {
      _ <-
        whilst (i < tiers.size && (cell.isEmpty || cell.get.key > limit)) {
          for (c <- tiers (i) .ceiling (desc, key)) yield {
            cell = c
            i += 1
          }}
    } yield {
      if (cell.isDefined && cell.get.key <= limit)
        TierCell (cell.get.key, cell.get.value)
      else
        TierCell (limit, None)
    }}

  def ceiling (key: Bytes, limit: Bytes): Async [TierCell] =
    read (key, limit)

  def get (key: Bytes): Async [Option [Bytes]] =
    read (key, key) .map (_.value)

  def put (key: Bytes, value: Bytes): Long = {
    readLock.lock()
    try {
      primary.put (key, Some (value))
      gen
    } finally {
      readLock.unlock()
    }}

  def delete (key: Bytes): Long = {
    readLock.lock()
    try {
      primary.put (key, None)
      gen
    } finally {
      readLock.unlock()
    }}

  def iterator: TierCellIterator = {
    readLock.lock()
    val (primary, secondary, tiers) = try {
      (this.primary, this.secondary, this.tiers)
    } finally {
      readLock.unlock()
    }
    TierIterator.merge (desc, primary, secondary, tiers) .dedupe
  }

  def probe (groups: Set [Long]): Set [Long] = {
    readLock.lock()
    val (tiers) = try {
      this.tiers
    } finally {
      readLock.unlock()
    }
    tiers.active
  }

  def compact (groups: Set [Long]): Async [Meta] =
    checkpoint()

  def checkpoint(): Async [Meta] = disks.join {

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
    for {
      tier <- TierBuilder.build (desc, obj, gen, iter)
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

  def apply [K, V] (desc: TierDescriptor [K,V], obj: ObjectId) (
      implicit scheduler: Scheduler, disk: Disks, config: StoreConfig): SynthTable [K, V] = {
    val lock = new ReentrantReadWriteLock
    new SynthTable (desc, obj, lock, 0, new MemTier, new MemTier, Tiers.empty)
  }}
