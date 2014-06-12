package com.treode.store.tier

import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.JavaConversions

import com.treode.async.{Async, AsyncIterator, Callback, Scheduler}
import com.treode.async.implicits._
import com.treode.disk.{Disk, PageDescriptor, Position}
import com.treode.store._

import Async.{async, guard, supply, when}
import Callback.ignore
import JavaConversions._
import Scheduler.toRunnable
import SynthTable.{genStepMask, genStepSize}
import TierTable.Meta

private class SynthTable (

    val desc: TierDescriptor,

    val id: TableId,

    // To lock the generation and references to the primary and secondary; this locks the references
    // only, while the skip list manages concurrent readers and writers of entries.  Writing to the
    // table requires only a read lock on the references to ensure that the compactor does not change
    // them. The compactor uses a write lock to move the primary to secondary, allocate a new
    // primary, and increment the generation.
    lock: ReentrantReadWriteLock,

    var gen: Long,

    // This resides in memory and it is the only tier that receives puts.
    var primary: MemTier,

    // This tier resides in memory and is being written to disk.
    var secondary: MemTier,

    // The position of each tier written on disk.
    var tiers: Tiers

) (implicit
    scheduler: Scheduler,
    disk: Disk,
    config: StoreConfig
) extends TierTable {
  import desc.pager
  import scheduler.whilst

  val readLock = lock.readLock()
  val writeLock = lock.writeLock()
  var queue = List.empty [Runnable]

  def typ = desc.id

  def get (key: Bytes, time: TxClock): Async [Cell] = disk.join {

    val mkey = Key (key, time)

    readLock.lock()
    val (primary, secondary, tiers) = try {
      (this.primary, this.secondary, this.tiers)
    } finally {
      readLock.unlock()
    }

    var candidate: Cell = null

    var entry = primary.ceilingEntry (mkey)
    if (entry != null) {
      val Key (k, t) = entry.getKey
      if (key == k && (candidate == null || candidate.time < t)) {
        candidate = memTierEntryToCell (entry)
      }}

    entry = secondary.ceilingEntry (mkey)
    if (entry != null) {
      val Key (k, t) = entry.getKey
      if (key == k && (candidate == null || candidate.time < t)) {
        candidate = memTierEntryToCell (entry)
      }}

    var i = 0
    whilst (i < tiers.size) {
      val tier = tiers (i)
      i += 1
      when (tier.earliest <= time && (candidate == null || candidate.time < tier.latest)) {
        tier.get (desc, key, time) .map {
         case Some (c @ Cell (k, t, v)) if key == k && (candidate == null || candidate.time < t) =>
            candidate = c
          case _ =>
            ()
        }}
    } .map { _ =>
      if (candidate == null)
        Cell (key, TxClock.MinValue, None)
      else
        candidate
    }}

  def put (key: Bytes, time: TxClock, value: Bytes): Long = {
    readLock.lock()
    try {
      primary.put (Key (key, time), Some (value))
      gen
    } finally {
      readLock.unlock()
    }}

  def delete (key: Bytes, time: TxClock): Long = {
    readLock.lock()
    try {
      primary.put (Key (key, time), None)
      gen
    } finally {
      readLock.unlock()
    }}

  def iterator (residents: Residents): CellIterator = {
    readLock.lock()
    val (primary, secondary, tiers) = try {
      (this.primary, this.secondary, this.tiers)
    } finally {
      readLock.unlock()
    }
    TierIterator
        .merge (desc, primary, secondary, tiers)
        .clean (desc, id, residents)
  }

  def iterator (start: Bound [Key], residents: Residents): CellIterator = {
    readLock.lock()
    val (primary, secondary, tiers) = try {
      (this.primary, this.secondary, this.tiers)
    } finally {
      readLock.unlock()
    }
    TierIterator
        .merge (desc, start, primary, secondary, tiers)
        .clean (desc, id, residents)
  }

  def receive (cells: Seq [Cell]): (Long, Seq [Cell]) = {
    readLock.lock()
    try {
      val novel = Seq.newBuilder [Cell]
      for {
        cell <- cells
        key = Key (cell.key, cell.time)
        if secondary.get (key) == null
        if primary.put (key, cell.value) == null
      } novel += cell
      (gen, novel.result)
    } finally {
      readLock.unlock()
    }}

  def probe (groups: Set [Long]): Async [Set [Long]] = async { cb =>
    writeLock.lock()
    try {
      if (!secondary.isEmpty)
        queue ::= toRunnable (probe (groups), cb)
      else
        cb.pass (tiers.active)
    } finally {
      writeLock.unlock()
    }}

  def compact(): Unit =
    pager.compact (id.id) run (ignore)

  def countKeys (tier: MemTier): Long = {
    var count = 0L
    var key: Bytes = null
    for (k <- tier.keySet; if k.key != k) {
      key = k.key
      count += 1
    }
    count
  }

  def compact (gen: Long, chosen: Tiers, residents: Residents): Async [Meta] = guard {
    val iter = TierIterator .merge (desc, chosen) .clean (desc, id, residents)
    val est = countKeys (primary) + chosen.estimate (residents)
    for {
      tier <- TierBuilder.build (desc, id, gen, est, residents, iter)
    } yield {
      writeLock.lock()
      try {
        tiers = tiers.compacted (tier, chosen)
        new Meta (math.max (gen, tiers.gen), tiers)
      } finally {
        writeLock.unlock()
      }}}

  def compact (groups: Set [Long], residents: Residents): Async [Meta] = async { cb =>
    writeLock.lock()
    try {
      if (!secondary.isEmpty) {
        queue ::= toRunnable (compact (groups, residents), cb)
      } else {
        val chosen = tiers.choose (groups, residents)
        if (chosen.isEmpty) {
          cb.pass (new Meta (tiers.gen, tiers))
        } else if (primary.isEmpty) {
          val gen = this.gen
          this.gen += genStepSize
          compact (gen, chosen, residents) run (cb)
        } else {
          val gen = tiers.gen + 1
          assert ((gen & genStepMask) != 0, "Tier compacted too many times")
          compact (gen, chosen, residents) run (cb)
        }}
    } finally {
      writeLock.unlock()
    }}

  def checkpoint (gen: Long, primary: MemTier, residents: Residents): Async [Meta] = guard {
    val iter = TierIterator .adapt (primary) .clean (desc, id, residents)
    val est = countKeys (primary)
    for {
      tier <- TierBuilder.build (desc, id, gen, est, residents, iter)
    } yield {
      writeLock.lock()
      val (meta, queue) = try {
        val q = this.queue
        this.queue = List.empty
        secondary = newMemTier
        tiers = tiers.compacted (tier, Tiers.empty)
        (new Meta (gen, tiers), q)
      } finally {
        writeLock.unlock()
      }
      queue foreach (_.run)
      meta
    }}

  def checkpoint (residents: Residents): Async [Meta] = guard {

    writeLock.lock()
    val (gen, primary, tiers) = try {
      assert (secondary.isEmpty, "Checkpoint already in progress.")
      val g = this.gen
      val p = this.primary
      if (!this.primary.isEmpty) {
        this.gen += genStepSize
        this.primary = secondary
        secondary = p
      }
      (g, p, this.tiers)
    } finally {
      writeLock.unlock()
    }

    if (primary.isEmpty) {
      supply (new Meta (tiers.gen, tiers))
    } else {
      checkpoint (gen, primary, residents)
    }}}

private object SynthTable {

  val genStepBits = 4
  val genStepSize = (1<<7).toLong
  val genStepMask = genStepSize - 1

  def apply (desc: TierDescriptor, id: TableId) (
      implicit scheduler: Scheduler, disk: Disk, config: StoreConfig): SynthTable = {
    val lock = new ReentrantReadWriteLock
    new SynthTable (desc, id, lock, genStepSize, newMemTier, newMemTier, Tiers.empty)
  }}
