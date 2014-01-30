package com.treode.store.simple

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.treode.async.{AsyncIterator, Callback, callback, delay}
import com.treode.disk.{Disks, PageHandler, PageDescriptor, Position, TypeId}
import com.treode.store.{Bytes, StoreConfig}

import SimpleTable.Meta

private class SynthTable (

    val pager: TierPage.Descriptor,

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

) (implicit disks: Disks, config: StoreConfig) extends SimpleTable with PageHandler [Long] {

  private val readLock = lock.readLock()
  private val writeLock = lock.writeLock()

  private def read (key: Bytes, cb: Callback [Option [Bytes]]) {

    readLock.lock()
    val (primary, secondary, tiers) = try {
      (this.primary, this.secondary, this.tiers)
    } finally {
      readLock.unlock()
    }

    val keyCell = SimpleCell (key, None)

    var cell = primary.floor (keyCell)
    if (cell != null && cell.key == key) {
      cb (cell.value)
      return
    }

    cell = secondary.floor (keyCell)
    if (cell != null && cell.key == key) {
      cb (cell.value)
      return
    }

    var i = 0
    val loop = new Callback [Option [SimpleCell]] {

      def pass (cell: Option [SimpleCell]) {
        cell match {
          case Some (cell) => cb (cell.value)
          case None =>
            i += 1
            if (i < tiers.size)
              TierReader.read (pager, tiers.pos (i), key, this)
            else
              cb (None)
        }}

      def fail (t: Throwable) = cb.fail (t)
    }

    if (i < tiers.size)
      TierReader.read (pager, tiers.pos (i), key, loop)
    else
      cb (None)
  }

  def get (key: Bytes, cb: Callback [Option [Bytes]]): Unit =
    read (key, cb)

  def iterator (cb: Callback [SimpleIterator]) {

    readLock.lock()
    val (primary, secondary, tiers) = try {
      (this.primary, this.secondary, this.tiers)
    } finally {
      readLock.unlock()
    }

    TierIterator.merge (pager, primary, secondary, tiers, cb)
  }

  def put (key: Bytes, value: Bytes): Long = {
    readLock.lock()
    try {
      primary.add (SimpleCell (key, Some (value)))
      generation
    } finally {
      readLock.unlock()
    }}

  def delete (key: Bytes): Long = {
    readLock.lock()
    try {
      primary.add (SimpleCell (key, None))
      generation
    } finally {
      readLock.unlock()
    }}

  def probe (groups: Set [Long]): Set [Long] =
    groups intersect tiers.active

  def compact (groups: Set [Long], cb: Callback [Unit]) {
    checkpoint (callback (cb) (_ => ()))
  }

  def checkpoint (cb: Callback [Meta]) {

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

    val built = delay (cb) { tier: Tier =>
      writeLock.lock()
      val meta = try {
        this.secondary = newMemTier
        this.tiers = Tiers (tier)
        new Meta (generation, this.tiers)
      } finally {
        writeLock.unlock()
      }
      cb (meta)
    }

    val merged = delay (cb) { iter: SimpleIterator =>
      TierBuilder.build (pager, generation, iter, built)
    }

    TierIterator.merge (pager, primary, emptyMemTier, tiers, merged)
  }}
