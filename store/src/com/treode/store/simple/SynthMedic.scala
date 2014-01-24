package com.treode.store.simple

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.treode.disk.{Disks, Position}
import com.treode.store.{Bytes, SimpleCell, StoreConfig}
import SimpleTable.{Medic, Meta}

private class SynthMedic (meta: Meta, config: StoreConfig) (implicit disks: Disks) extends Medic {

  private val lock = new ReentrantReadWriteLock
  private val readLock = lock.readLock()
  private val writeLock = lock.writeLock()

  private var generation = 0L

  // This resides in memory and it is the only tier that is written.
  private var primary = newMemTable

  // This tier resides in memory and is being compacted and written to disk.
  private var secondary = newMemTable

  // The position of each tier on disk.
  private var tiers = Array [Position] ()

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
          primary = newMemTable
          primary.add (cell)
        } else if (gen > this.generation + 1) {
          this.generation = gen
          primary = newMemTable
          secondary = newMemTable
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
        secondary = newMemTable
      }
      val t = new SynthTable (config, lock, generation, primary, secondary, tiers)
      primary = null
      secondary = null
      t
    } finally {
      writeLock.unlock()
    }}
}
