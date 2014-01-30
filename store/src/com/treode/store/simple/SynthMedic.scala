package com.treode.store.simple

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.treode.disk.{Launch, Position, Recovery, TypeId}
import com.treode.store.{Bytes, StoreConfig}

import SimpleTable.Meta

private class SynthMedic (id: TypeId) (implicit recovery: Recovery, config: StoreConfig)
extends SimpleMedic {

  private val lock = new ReentrantReadWriteLock
  private val readLock = lock.readLock()
  private val writeLock = lock.writeLock()

  private var generation = 0L

  // This resides in memory and it is the only tier that is written.
  private var primary = newMemTier

  // This tier resides in memory and is being compacted and written to disk.
  private var secondary = newMemTier

  // The position of each tier on disk.
  private var tiers = Tiers.empty

  private def replay (gen: Long, key: Bytes, value: Option [Bytes]) {

    val cell = SimpleCell (key, value)

    readLock.lock()
    val needWrite = try {
      if (gen == this.generation - 1) {
        secondary.add (cell)
        true
      } else if (gen == this.generation) {
        primary.add (cell)
        true
      } else {
        false
      }
    } finally {
      readLock.unlock()
    }

    if (needWrite) {
      writeLock.lock()
      try {
        if (gen == this.generation - 1) {
          secondary.add (cell)
        } else if (gen == this.generation) {
          primary.add (cell)
        } else if (gen == this.generation + 1) {
          this.generation = gen
          secondary = primary
          primary = newMemTier
          primary.add (cell)
        } else if (gen > this.generation + 1) {
          this.generation = gen
          primary = newMemTier
          secondary = newMemTier
          primary.add (cell)
        }
      } finally {
        writeLock.unlock()
      }}}

  def put (gen: Long, key: Bytes, value: Bytes): Unit =
    replay (gen, key, Some (value))

  def delete (gen: Long, key: Bytes): Unit =
    replay (gen, key, None)

  def checkpoint (meta: SimpleTable.Meta) {
    writeLock.lock()
    try {
      generation = meta.gen+1
      primary = newMemTier
      secondary = newMemTier
      tiers = meta.tiers
    } finally {
      writeLock.unlock()
    }}

  def close () (implicit launcher: Launch): SimpleTable = {
    import launcher.disks

    writeLock.lock()
    val (generation, primary, secondary, tiers) = try {
      if (!this.secondary.isEmpty) {
        this.secondary.addAll (this.primary)
        this.primary = this.secondary
        this.secondary = newMemTier
      }
      val result = (this.generation, this.primary, this.secondary, this.tiers)
      this.primary = null
      this.secondary = null
      this.tiers = null
      result
    } finally {
      writeLock.unlock()
    }

    val pager = TierPage.pager (id)
    val table = new SynthTable (pager, lock, generation, primary, secondary, tiers)
    pager.handle (table)
    table
  }
}
