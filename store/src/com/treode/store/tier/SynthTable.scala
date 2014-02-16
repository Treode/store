package com.treode.store.tier

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.treode.async.{Async, AsyncIterator, Callback, Scheduler, callback, continue}
import com.treode.disk.{Disks, PageHandler, PageDescriptor, Position, TypeId}
import com.treode.store.{Bytes, StoreConfig}

import Async.async
import TierTable.Meta

private class SynthTable [K, V] (

    val desc: TierDescriptor [K, V],

    // To lock the generation and references to the primary and secondary; this locks the references
    // only, while the skip list manages concurrent readers and writers of entries.  Writing to the
    // table requires only a read lock on the references to ensure that the compactor does not change
    // them. The compactor uses a write lock to move the primary to secondary, allocate a new
    // primary, and increment the generation.
    lock: ReentrantReadWriteLock,

    var generation: Long,

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
) extends TierTable with PageHandler [Long] {

  private val readLock = lock.readLock()
  private val writeLock = lock.writeLock()

  private def read (key: Bytes, cb: Callback [Option [Bytes]]) {

    val epoch = disks.join (cb)

    readLock.lock()
    val (primary, secondary, tiers) = try {
      (this.primary, this.secondary, this.tiers)
    } finally {
      readLock.unlock()
    }

    var cell = primary.floorEntry (key)
    if (cell != null && cell.getKey == key) {
      epoch (cell.getValue)
      return
    }

    cell = secondary.floorEntry (key)
    if (cell != null && cell.getKey == key) {
      epoch (cell.getValue)
      return
    }

    var i = 0
    val loop = new Callback [Option [Cell]] {

      def pass (cell: Option [Cell]) {
        cell match {
          case Some (cell) => epoch (cell.value)
          case None =>
            i += 1
            if (i < tiers.size)
              tiers (i) .read (desc, key, this)
            else
              epoch (None)
        }}

      def fail (t: Throwable) = epoch.fail (t)
    }

    if (i < tiers.size)
      tiers (i) .read (desc, key, loop)
    else
      epoch (None)
  }

  def get (key: Bytes, cb: Callback [Option [Bytes]]): Unit =
    read (key, cb)

  def put (key: Bytes, value: Bytes): Long = {
    readLock.lock()
    try {
      primary.put (key, Some (value))
      generation
    } finally {
      readLock.unlock()
    }}

  def delete (key: Bytes): Long = {
    readLock.lock()
    try {
      primary.put (key, None)
      generation
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
    val merged = TierIterator.merge (desc, primary, secondary, tiers)
    OverwritesFilter (merged)
  }

  def probe (groups: Set [Long], cb: Callback [Set [Long]]): Unit =
    cb (groups intersect tiers.active)

  def compact (groups: Set [Long], cb: Callback [Unit]) {
    checkpoint (callback (cb) (_ => ()))
  }

  def checkpoint (cb: Callback [Meta]) {

    val epoch = disks.join (cb)

    writeLock.lock()
    val (generation, primary, tiers) = try {
      require (secondary.isEmpty)
      val g = this.generation
      val p = this.primary
      this.generation += 1
      this.primary = secondary
      this.secondary = p
      (g, p, this.tiers)
    } finally {
      writeLock.unlock()
    }

    val built = continue (epoch) { tier: Tier =>
      writeLock.lock()
      val meta = try {
        this.secondary = newMemTier
        this.tiers = Tiers (tier)
        new Meta (generation, this.tiers)
      } finally {
        writeLock.unlock()
      }
      epoch (meta)
    }

    val merged = TierIterator.merge (desc, primary, emptyMemTier, tiers)
    val filtered = OverwritesFilter (merged)
    TierBuilder.build (desc, generation, filtered) .run (built)
  }}

private object SynthTable {

  def apply [K, V] (desc: TierDescriptor [K,V]) (
      implicit scheduler: Scheduler, disk: Disks, config: StoreConfig): SynthTable [K, V] = {
    val lock = new ReentrantReadWriteLock
    new SynthTable (desc, lock, 0, new MemTier, new MemTier, Tiers.empty)
  }}
