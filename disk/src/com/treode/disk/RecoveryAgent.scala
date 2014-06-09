package com.treode.disk

import java.nio.file.Path

import com.treode.async.{Async, Scheduler}
import com.treode.async.implicits._
import com.treode.async.io.File

import Async.guard
import Disk.Launch
import SuperBlocks.{chooseSuperBlock, verifyReattachment}

private class RecoveryAgent (implicit scheduler: Scheduler, config: DiskConfig)
extends Disk.Recovery {
  import config.cell

  private val records = new RecordRegistry
  private var open = true

  def requireOpen(): Unit =
    require (open, "Recovery has already begun.")

  def replay [R] (desc: RecordDescriptor [R]) (f: R => Any): Unit =
    synchronized {
      requireOpen()
      records.replay (desc) (f)
    }

  def close(): Unit =
    synchronized {
      requireOpen()
      open = false
    }

  def _attach (items: (Path, File, DiskGeometry)*): Async [Launch] =
    guard {

      val attaching = items.map (_._1) .toSet
      require (!items.isEmpty, "Must list at least one file or device to attach.")
      require (attaching.size == items.size, "Cannot attach a path multiple times.")
      close()

      val kit = new DiskKit (0)
      val boot = BootBlock.apply (cell, 0, items.size, attaching)
      for {
        drives <-
          for (((path, file, geometry), i) <- items.zipWithIndex.latch.seq)
            DiskDrive.init (i, path, file, geometry, boot, kit)
        _ <- kit.disks.add (drives)
      } yield {
        new LaunchAgent (kit)
      }}

  def attach (items: (Path, DiskGeometry)*): Async [Launch] =
    guard {
      val files =
        for ((path, geom) <- items)
          yield (path, openFile (path, geom), geom)
      _attach (files: _*)
    }

  def reopen (items: Seq [(Path, File)]): Async [Seq [SuperBlocks]] =
    for ((path, file) <- items.latch.casual)
      SuperBlocks.read (path, file)

  def reopen (path: Path): Async [SuperBlocks] =
    guard {
      val file = reopenFile (path)
      SuperBlocks.read (path, file)
    }

  def reopen (items: Seq [Path]) (_reopen: Path => Async [SuperBlocks]): Async [Seq [SuperBlocks]] = {

    var opening = items.toSet
    var opened = Set.empty [Path]
    var superbs = Seq.empty [SuperBlocks]

    scheduler.whilst (!opening.isEmpty) {
      for {
        _superbs <- opening.latch.casual foreach (_reopen)
      } yield {
        opened ++= _superbs map (_.path)
        superbs ++= _superbs
        val useGen0 = chooseSuperBlock (superbs)
        val boot = superbs.head.superb (useGen0) .boot
        val expecting = boot.disks.toSet
        opening = expecting -- opened
      }
    } .map { _ =>
      superbs
    }}

  def _reattach (items: (Path, File)*): Async [Launch] =
    guard {

      val reattaching = items.map (_._1) .toSet
      require (!items.isEmpty, "Must list at least one file or device to reaattach.")
      require (reattaching.size == items.size, "Cannot reattach a path multiple times.")
      close()

      for {
        superbs <- reopen (items)
        _ = verifyReattachment (superbs)
        kit <- LogIterator.replay (superbs, records)
      } yield {
        new LaunchAgent (kit)
      }}

  def _reattach (items: Seq [Path]) (_reopen: Path => Async [SuperBlocks]): Async [Launch] =
    guard {
      require (!items.isEmpty, "Must list at least one file or device to reaattach.")
      close()
      for {
        superbs <- reopen (items) (_reopen)
        _ = verifyReattachment (superbs)
        kit <- LogIterator.replay (superbs, records)
      } yield {
        new LaunchAgent (kit)
      }}

  def reattach (items: Path*): Async [Launch] =
    _reattach (items) (reopen _)
}
