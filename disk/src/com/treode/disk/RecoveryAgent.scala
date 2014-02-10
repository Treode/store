package com.treode.disk

import java.nio.file.Path
import java.util.ArrayList
import java.util.concurrent.ExecutorService

import com.treode.async.{Callback, Scheduler, continue, defer}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

private class RecoveryAgent (
    records: RecordRegistry,
    loaders: ReloadRegistry,
    launches: ArrayList [Launch => Any],
    val cb: Callback [Disks]
) (implicit
    val scheduler: Scheduler,
    val config: DisksConfig
) {

  def launch (disks: DiskDrives): Unit =
    defer (cb) {
      new LaunchAgent (disks, launches, cb)
    }

  def attach (items: Seq [(Path, File, DiskGeometry)]): Unit =
    defer (cb) {

      val logd = new Dispatcher [PickledRecord] (scheduler)
      val paged = new Dispatcher [PickledPage] (scheduler)

      val disksPrimed = continue (cb) { drives: Seq [DiskDrive] =>
        val disks = new DiskDrives (logd, paged, drives.mapBy (_.id))
        launch (disks)
      }

      val attaching = items.map (_._1) .toSet
      val roots = Position (0, 0, 0)
      val latch = Callback.seq (items.size, disksPrimed)
      val boot = BootBlock.apply (0, items.size, attaching, roots)
      DiskDrive.init (items, 0, boot, logd, paged, disksPrimed)
    }

  def attach (items: Seq [(Path, DiskGeometry)], exec: ExecutorService): Unit =
    defer (cb) {
      val files = items map (openFile (_, exec))
      attach (files)
    }

  def chooseSuperBlock (reads: Seq [SuperBlocks]): Boolean = {

    val sb1 = reads.map (_.sb1) .flatten
    val sb2 = reads.map (_.sb2) .flatten
    if (sb1.size == 0 && sb2.size == 0)
      throw new NoSuperBlocksException

    val gen1 = if (sb1.isEmpty) -1 else sb1.map (_.boot.gen) .max
    val n1 = sb1 count (_.boot.gen == gen1)
    val gen2 = if (sb2.isEmpty) -1 else sb2.map (_.boot.gen) .max
    val n2 = sb2 count (_.boot.gen == gen2)
    if (n1 != reads.size && n2 != reads.size)
      throw new InconsistentSuperBlocksException

    (n1 == reads.size) && (gen1 > gen2 || n2 != reads.size)
  }

  def verifyReattachment (booted: Set [Path], reattaching: Set [Path]) {
    if (!(booted forall (reattaching contains _))) {
      val missing = (booted -- reattaching).toSeq.sorted
      throw new MissingDisksException (missing)
    }
    if (!(reattaching forall (booted contains _))) {
      val extra = (reattaching -- booted).toSeq.sorted
      new ExtraDisksException (extra)
    }}

  def superBlocksRead (reads: Seq [SuperBlocks]): Unit =
    defer (cb) {

      val useGen1 = chooseSuperBlock (reads)
      val boot = if (useGen1) reads.head.sb1.get.boot else reads.head.sb2.get.boot
      verifyReattachment (boot.disks.toSet, reads .map (_.path) .toSet)

      val files = reads.mapValuesBy (_.superb (useGen1) .id) (_.file)

      val logsReplayed = continue (cb) { disks: DiskDrives =>
        launch (disks)
      }

      val rootsReloaded = continue (cb) { _: Unit =>
        LogIterator.replay (useGen1, reads, records, logsReplayed)
      }

      val rootsRead = continue (cb) { roots: Seq [Reload => Any] =>
        new ReloadAgent (files, roots, rootsReloaded)
      }

      val roots = reads.head.superb (useGen1) .boot.roots
      if (roots.length == 0)
        rootsRead (Seq.empty)
      else
        DiskDrive.read (files (roots.disk), loaders.pager, roots, rootsRead)
    }

  def reattach (items: Seq [(Path, File)]): Unit =
    defer (cb) {
      require (!items.isEmpty, "Must list at least one file to reaattach.")
      val oneRead = Callback.seq (items.size, continue (cb) (superBlocksRead _))
      for ((path, file) <- items)
        SuperBlocks.read (path, file, oneRead)
    }

  def reattach (items: Seq [Path], exec: ExecutorService): Unit =
    defer (cb) {
      reattach (items map (reopenFile (_, exec)))
    }}
