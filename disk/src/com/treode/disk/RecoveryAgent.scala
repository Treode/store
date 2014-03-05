package com.treode.disk

import java.nio.file.Path
import java.util.ArrayList
import java.util.concurrent.ExecutorService
import scala.language.postfixOps

import com.treode.async.{Async, AsyncConversions, Callback, Scheduler}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

import Async.{async, defer, supply}
import AsyncConversions._

private class RecoveryAgent (
    records: RecordRegistry,
    loaders: ReloadRegistry,
    val cb: Callback [Disks.Launch]
) (implicit
    val scheduler: Scheduler,
    val config: DisksConfig
) {

  def launch (disks: DiskDrives): Unit =
    cb.invoke {
      new LaunchAgent (disks)
    }

  def attach (items: Seq [(Path, File, DiskGeometry)]): Unit =
    cb.defer {
      require (!items.isEmpty, "Must list at least one file or device to attach.")

      val disks = new DiskDrives
      val attaching = items.map (_._1) .toSet
      val roots = Position (0, 0, 0)
      val boot = BootBlock.apply (0, items.size, attaching, 0, roots)

      val task = for {
        drives <- items.latch.indexed { case ((path, file, geometry), i) =>
          DiskDrive.init (i, path, file, geometry, boot, disks)
        }
        _ <- disks.add (drives)
      } yield launch (disks)
      task run disks.panic
    }

  def attach (items: Seq [(Path, DiskGeometry)], exec: ExecutorService): Unit =
    cb.defer {
      val files = items map (openFile (_, exec))
      attach (files)
    }

  def chooseSuperBlock (reads: Seq [SuperBlocks]): Boolean = {

    val sb1 = reads.map (_.sb1) .flatten
    val sb2 = reads.map (_.sb2) .flatten
    if (sb1.size == 0 && sb2.size == 0)
      throw new NoSuperBlocksException

    val gen1 = if (sb1.isEmpty) -1 else sb1.map (_.boot.bootgen) .max
    val n1 = sb1 count (_.boot.bootgen == gen1)
    val gen2 = if (sb2.isEmpty) -1 else sb2.map (_.boot.bootgen) .max
    val n2 = sb2 count (_.boot.bootgen == gen2)
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
      val rootpos = reads.head.superb (useGen1) .boot.roots
      val files = reads.mapValuesBy (_.superb (useGen1) .id) (_.file)

      for {
        roots <-
            if (rootpos.length == 0)
              supply (Seq.empty [Disks.Reload => Async [Unit]])
            else
              DiskDrive.read (files (rootpos.disk), loaders.pager, rootpos)
        _ <- async [Unit] (new ReloadAgent (files, roots, _))
        disks <- LogIterator.replay (useGen1, reads, records)
      } yield launch (disks)
    }

  def reattach (items: Seq [(Path, File)]): Unit =
    defer (cb) {
      require (!items.isEmpty, "Must list at least one file or device to reaattach.")
      for {
        reads <- items.latch.seq { case (path, file) => SuperBlocks.read (path, file) }
      } yield superBlocksRead (reads)
    }

  def reattach (items: Seq [Path], exec: ExecutorService): Unit =
    cb.defer {
      reattach (items map (reopenFile (_, exec)))
    }}
