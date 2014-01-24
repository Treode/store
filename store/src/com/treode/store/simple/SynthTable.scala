package com.treode.store.simple

import java.util.Collections
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.treode.async.{AsyncIterator, Callback, callback, delay}
import com.treode.disk.{Disks, Position}
import com.treode.store.{Bytes, SimpleCell, StoreConfig}

private class SynthTable (config: StoreConfig) (implicit disks: Disks) extends SimpleTable {

  // To lock the generation and references to the primary and secondary; this locks the references
  // only, while the skip list manages concurrent readers and writers of entries.  Writing to the
  // table requires only a read lock on the references to ensure that the compactor does not change
  // them. The compactor uses a write lock to move the primary to secondary, allocate a new
  // primary, and increment the generation.
  private val lock = new ReentrantReadWriteLock
  private val readLock = lock.readLock()
  private val writeLock = lock.writeLock()

  private var generation = 0L

  private def newMemTable = new ConcurrentSkipListSet [SimpleCell] (SimpleCell)

  // This resides in memory and it is the only tier that is written.
  private var primary = newMemTable

  // This tier resides in memory and is being compacted and written to disk.
  private var secondary = newMemTable

  // The position of each tier on disk.
  private var tiers = Array [Position] ()

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

  def put (gen: Long, key: Bytes, value: Bytes): Unit =
    replay (gen, key, Some (value))

  def delete (key: Bytes): Long = {
    readLock.lock()
    try {
      primary.add (SimpleCell (key, None))
      generation
    } finally {
      readLock.unlock()
    }}

  def checkpoint (cb: Callback [Unit]) {

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
      try {
        this.secondary = newMemTable
        this.tiers = Array (pos)
      } finally {
        writeLock.unlock()
      }
      cb()
    }

    val merged = delay (cb) { iter: SimpleIterator =>
      TierBuilder.build (config, iter, built)
    }

    TierIterator.merge (primary, Collections.emptyList(), tiers, merged)
  }

  private def replay (gen: Long, key: Bytes, value: Option [Bytes]) {
    val cell = SimpleCell (key, value)
    if (gen == this.generation - 1) {
      secondary.add (cell)
    } else if (gen == this.generation) {
      primary.add (cell)
    } else if (gen == this.generation + 1) {
      this.generation = gen
      secondary = primary
      primary = newMemTable
      primary.add (cell)
    } else if (gen > this.generation + 1) {
      this.generation = gen
      primary = newMemTable
      secondary = newMemTable
      primary.add (cell)
    }}

  def deleted (gen: Long, key: Bytes): Unit =
    replay (gen, key, None)
}
