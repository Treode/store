package com.treode.store.simple

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.treode.disk.{Disks, Position, TypeId}
import com.treode.store.{Bytes, StoreConfig}

import SimpleTable.Meta

private class SynthMedic (id: TypeId) (implicit disks: Disks, config: StoreConfig)
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

  private def recover (meta: SimpleTable.Meta) {
    writeLock.lock()
    try {
      generation = meta.gen+1
      primary = newMemTier
      secondary = newMemTier
      tiers = meta.tiers
    } finally {
      writeLock.unlock()
    }}

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

  def close(): SimpleTable = {
    writeLock.lock()
    try {
      if (!secondary.isEmpty) {
        secondary.addAll (primary)
        primary = secondary
        secondary = newMemTier
      }
      val t = new SynthTable (id, lock, generation, primary, secondary, tiers)
      primary = null
      secondary = null
      t
    } finally {
      writeLock.unlock()
    }}
}
