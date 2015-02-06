/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.disk

import java.nio.file.Path

import com.treode.async.{Async, Scheduler}
import com.treode.async.implicits._
import com.treode.async.io.File

import Async.guard
import Disk.Launch
import SuperBlocks.{chooseSuperBlock, verifyReattachment}

private class RecoveryAgent (implicit scheduler: Scheduler, config: Disk.Config)
extends Disk.Recovery {

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

  def _attach (sysid: Array [Byte], items: (Path, File, DriveGeometry)*): Async [Launch] =
    guard {

      val attaching = items.map (_._1) .toSet
      require (!items.isEmpty, "Must list at least one file or device to attach.")
      require (attaching.size == items.size, "Cannot attach a path multiple times.")
      items foreach (_._3.validForConfig())
      close()

      val kit = new DiskKit (sysid, 0)
      val boot = BootBlock.apply (sysid, 0, items.size, attaching)
      for {
        drives <-
          for (((path, file, geometry), i) <- items.indexed)
            yield DiskDrive.init (i, path, file, geometry, boot, kit)
        _ <- kit.drives.add (drives)
      } yield {
        new LaunchAgent (kit)
      }}

  def attach (sysid: Array [Byte], items: (Path, DriveGeometry)*): Async [Launch] =
    guard {
      val files =
        for ((path, geom) <- items)
          yield (path, openFile (path, geom), geom)
      _attach (sysid, files: _*)
    }

  def reopen (items: Seq [(Path, File)]): Async [Seq [SuperBlocks]] =
    for ((path, file) <- items.latch.collect)
      yield SuperBlocks.read (path, file)

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
        _superbs <- opening.latch.collect (_reopen)
      } yield {
        opened ++= _superbs map (_.path)
        superbs ++= _superbs
        val useGen0 = chooseSuperBlock (superbs)
        val boot = superbs.head.superb (useGen0) .boot
        val expecting = boot.drives.toSet
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
