package com.treode.store.simple

import java.util.Collections
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.treode.async.{AsyncIterator, Callback, callback, delay}
import com.treode.disk.{Disks, Position}
import com.treode.store.{Bytes, SimpleCell, StoreConfig}

import SimpleTable.Meta

private class SynthTable (
    config: StoreConfig,

    // To lock the generation and references to the primary and secondary; this locks the references
    // only, while the skip list manages concurrent readers and writers of entries.  Writing to the
    // table requires only a read lock on the references to ensure that the compactor does not change
    // them. The compactor uses a write lock to move the primary to secondary, allocate a new
    // primary, and increment the generation.
    lock: ReentrantReadWriteLock,

    var generation: Long,

    // This resides in memory and it is the only tier that is written.
    var primary: MemTable,

    // This tier resides in memory and is being compacted and written to disk.
    var secondary: MemTable,

    // The position of each tier on disk.
    var tiers: Array [Position]

) (implicit disks: Disks) extends SimpleTable {

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
            if (i < tiers.length)
              Tier.read (tiers (i), key, this)
            else
              cb (None)
        }}

      def fail (t: Throwable) = cb.fail (t)
    }

    Tier.read (tiers (i), key, loop)
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

    TierIterator.merge (primary, secondary, tiers, cb)
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

    val built = delay (cb) { pos: Position =>
      writeLock.lock()
      val meta = try {
        this.secondary = newMemTable
        this.tiers = Array (pos)
        Meta (this.generation, this.tiers)
      } finally {
        writeLock.unlock()
      }
      cb (meta)
    }

    val merged = delay (cb) { iter: SimpleIterator =>
      TierBuilder.build (config, iter, built)
    }

    TierIterator.merge (primary, Collections.emptyList(), tiers, merged)
  }}
