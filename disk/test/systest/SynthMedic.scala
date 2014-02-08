package systest

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.treode.disk.{Disks, Position, Recovery, TypeId}

class SynthMedic (implicit recovery: Recovery, config: TestConfig) {

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

  def update (gen: Long, key: Int, value: Option [Int]) {

    val cell = Cell (key, value)

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

  def compact (tiers: Tiers) {
    writeLock.lock()
    try {
      this.generation = tiers.gen+1
      this.primary = newMemTier
      this.secondary = newMemTier
      this.tiers = tiers
    } finally {
      writeLock.unlock()
    }}

  def reload (tiers: Tiers): Unit =
    compact (tiers)

  def close () (implicit disks: Disks): SynthTable = {

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

    new SynthTable (lock, generation, primary, secondary, tiers)
  }}
