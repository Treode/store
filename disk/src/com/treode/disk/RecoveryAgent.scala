package com.treode.disk

import java.nio.file.Path
import java.util.ArrayList
import java.util.concurrent.ExecutorService
import scala.language.postfixOps

import com.treode.async.{Async, AsyncConversions, Callback, Scheduler}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

import Async.{async, guard, supply}
import AsyncConversions._

private class RecoveryAgent (
    records: RecordRegistry,
    val cb: Callback [Disks.Launch]
) (implicit
    val scheduler: Scheduler,
    val config: DisksConfig
) {

  def launch (kit: DisksKit): Unit =
    cb.invoke {
      new LaunchAgent (kit)
    }

  def attach (items: Seq [(Path, File, DiskGeometry)]): Unit =
    cb.defer {
      require (!items.isEmpty, "Must list at least one file or device to attach.")

      val kit = new DisksKit
      val attaching = items.map (_._1) .toSet
      val boot = BootBlock.apply (0, 0, items.size, attaching)

      val task = for {
        drives <- items.latch.indexed { case ((path, file, geometry), i) =>
          DiskDrive.init (i, path, file, geometry, boot, kit)
        }
        _ <- kit.disks.add (drives)
      } yield launch (kit)
      task run kit.panic
    }

  def attach (items: Seq [(Path, DiskGeometry)], exec: ExecutorService): Unit =
    cb.defer {
      val files = items map (openFile (_, exec))
      attach (files)
    }

  def chooseSuperBlock (reads: Seq [SuperBlocks]): Boolean = {

    val sb0 = reads.map (_.sb0) .flatten
    val sb1 = reads.map (_.sb1) .flatten
    if (sb0.size == 0 && sb1.size == 0)
      throw new NoSuperBlocksException

    val gen0 = if (sb0.isEmpty) -1 else sb0.map (_.boot.gen) .max
    val n0 = sb0 count (_.boot.gen == gen0)
    val gen1 = if (sb1.isEmpty) -1 else sb1.map (_.boot.gen) .max
    val n1 = sb1 count (_.boot.gen == gen1)
    if (n0 != reads.size && n1 != reads.size)
      throw new InconsistentSuperBlocksException

    (n0 == reads.size) && (gen0 > gen1 || n1 != reads.size)
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
    guard {

      val useGen0 = chooseSuperBlock (reads)
      val boot = if (useGen0) reads.head.sb0.get.boot else reads.head.sb1.get.boot
      verifyReattachment (boot.disks.toSet, reads .map (_.path) .toSet)
      val files = reads.mapValuesBy (_.superb (useGen0) .id) (_.file)

      for (kit <- LogIterator.replay (useGen0, reads, records))
        yield launch (kit)
    } defer (cb)

  def reattach (items: Seq [(Path, File)]): Unit =
    guard {
      require (!items.isEmpty, "Must list at least one file or device to reaattach.")
      for {
        reads <- items.latch.seq { case (path, file) => SuperBlocks.read (path, file) }
      } yield superBlocksRead (reads)
    } defer (cb)

  def reattach (items: Seq [Path], exec: ExecutorService): Unit =
    cb.defer {
      reattach (items map (reopenFile (_, exec)))
    }}
