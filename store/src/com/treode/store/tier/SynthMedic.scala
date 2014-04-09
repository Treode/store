package com.treode.store.tier

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.treode.async.Scheduler
import com.treode.disk.{Disks, ObjectId, Position}
import com.treode.store.{Bytes, StoreConfig, TxClock}

private class SynthMedic (
    desc: TierDescriptor,
    obj: ObjectId
) (implicit
    scheduler: Scheduler,
    config: StoreConfig
) extends TierMedic {

  private val lock = new ReentrantReadWriteLock
  private val readLock = lock.readLock()
  private val writeLock = lock.writeLock()

  private var gen = 0L

  // This resides in memory and it is the only tier that is written.
  private var primary = newMemTier

  // This tier resides in memory and is being compacted and written to disk.
  private var secondary = newMemTier

  // The position of each tier on disk.
  private var tiers = Tiers.empty

  private def replay (gen: Long, key: Bytes, time: TxClock, value: Option [Bytes]) {

    readLock.lock()
    val needWrite = try {
      if (gen == this.gen - 1) {
        secondary.put (MemKey (key, time), value)
        false
      } else if (gen == this.gen) {
        primary.put (MemKey (key, time), value)
        false
      } else {
        true
      }
    } finally {
      readLock.unlock()
    }

    if (needWrite) {
      writeLock.lock()
      try {
        if (gen == this.gen - 1) {
          secondary.put (MemKey (key, time), value)
        } else if (gen == this.gen) {
          primary.put (MemKey (key, time), value)
        } else if (gen == this.gen + 1) {
          this.gen = gen
          secondary = primary
          primary = newMemTier
          primary.put (MemKey (key, time), value)
        } else if (gen > this.gen + 1) {
          this.gen = gen
          primary = newMemTier
          secondary = newMemTier
          primary.put (MemKey (key, time), value)
        }
      } finally {
        writeLock.unlock()
      }}}

  def put (gen: Long, key: Bytes, time: TxClock, value: Bytes): Unit =
    replay (gen, key, time, Some (value))

  def delete (gen: Long, key: Bytes, time: TxClock): Unit =
    replay (gen, key, time, None)

  def checkpoint (meta: TierTable.Meta) {
    writeLock.lock()
    try {
      gen = meta.gen+1
      primary = newMemTier
      secondary = newMemTier
      tiers = meta.tiers
    } finally {
      writeLock.unlock()
    }}

  def close () (implicit launch: Disks.Launch): TierTable = {
    import launch.disks

    writeLock.lock()
    val (gem, primary, secondary, tiers) = try {
      if (!this.secondary.isEmpty) {
        this.secondary.putAll (this.primary)
        this.primary = this.secondary
        this.secondary = newMemTier
      }
      val result = (this.gen, this.primary, this.secondary, this.tiers)
      this.primary = null
      this.secondary = null
      this.tiers = null
      result
    } finally {
      writeLock.unlock()
    }

    new SynthTable (desc, obj, lock, gem, primary, secondary, tiers)
  }}
