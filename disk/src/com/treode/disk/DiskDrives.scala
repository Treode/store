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
import scala.collection.immutable.Queue
import scala.language.postfixOps
import scala.util.{Failure, Success}

import com.treode.async.{Async, AsyncQueue, Callback, Fiber}
import com.treode.async.implicits._
import com.treode.async.io.File

import Async.{async, guard, supply}
import Callback.{fanout, ignore}

private class DiskDrives (kit: DiskKit) {
  import kit.{checkpointer, compactor, config, scheduler, sysid}

  type AttachItem = (Path, File, DriveGeometry)
  type AttachRequest = (Seq [AttachItem], Callback [Unit])
  type CloseRequest = Callback [Unit]
  type DrainRequest = (Seq [Path], Callback [Unit])
  type DetachRequest = DiskDrive
  type CheckpointRequest = (Map [Int, Long], Callback [Unit])

  val fiber = new Fiber

  var drives = Map.empty [Int, DiskDrive]
  var closereqs = List.empty [CloseRequest]
  var attachreqs = Queue.empty [AttachRequest]
  var detachreqs = List.empty [DetachRequest]
  var drainreqs = Queue.empty [DrainRequest]
  var checkreqs = Queue.empty [CheckpointRequest]

  var closed = false
  var bootgen = 0
  var number = 0

  val queue = AsyncQueue (fiber) {
    if (closed) {
      // noop
    } else if (!closereqs.isEmpty) {
      val cb = fanout [Unit] (closereqs)
      closereqs = List.empty
      _close (cb)
    } else if (!attachreqs.isEmpty) {
      val ((items, cb), rest) = attachreqs.dequeue
      attachreqs = rest
      _attach (items, cb)
    } else if (!detachreqs.isEmpty) {
      val all = detachreqs
      detachreqs = List.empty
      _detach (all)
    } else if (!drainreqs.isEmpty) {
      val ((items, cb), rest) = drainreqs.dequeue
      drainreqs = rest
      _drain (items, cb)
    } else if (!checkreqs.isEmpty) {
      val ((marks, cb), rest) = checkreqs.dequeue
      checkreqs = rest
      _checkpoint (marks, cb)
    } else {
      None
    }}

  def launch(): Async [Unit] = {
    val attached = drives.values.setBy (_.path)
    val draining = drives.values.filter (_.draining)
    for {
      segs <- draining.latch.collect (_.drain())
    } yield {
      compactor.drain (segs.flatten)
      queue.launch()
      log.openedDrives (attached)
      if (!draining.isEmpty)
        log.drainingDrives (draining.setBy (_.path))
    }}

  def _close (cb: Callback [Unit]): Unit =
    queue.run (cb) {
      closed = true
      drives.values.latch (_.close())
    }

  def close(): Async [Unit] =
    queue.async { cb =>
      closereqs ::= cb
    }

  def digest: Async [Seq [DriveDigest]] =
    for {
      _drives <- fiber.supply (drives.values)
      _digests <- _drives.latch.collect (_.digest)
    } yield {
      _digests
    }

  private def _attach (items: Seq [AttachItem], cb: Callback [Unit]): Unit =
    queue.run (cb) {

      val priorDisks = drives.values
      val priorPaths = priorDisks.setBy (_.path)
      val newPaths = items.setBy (_._1)
      val bootgen = this.bootgen + 1
      val number = this.number + items.size
      val attached = priorPaths ++ newPaths
      val newBoot = BootBlock (sysid, bootgen, number, attached)

      if (newPaths exists (priorPaths contains _)) {
        val already = (newPaths intersect priorPaths).toSeq.sorted
        throw new ControllerException (s"Disks already attached: ${already mkString ", "}")
      }

      for {
        newDisks <-
          for (((path, file, geometry), i) <- items.indexed)
            yield DiskDrive.init (this.number + i + 1, path, file, geometry, newBoot, kit)
        _ <- priorDisks.latch (_.checkpoint (newBoot, None))
      } yield {
        drives ++= newDisks.mapBy (_.id)
        this.bootgen = bootgen
        this.number = number
        newDisks foreach (_.added())
        log.attachedDrives (newDisks.setBy (_.path))
      }}

  def _attach (items: Seq [AttachItem]): Async [Unit] = {
    queue.async { cb =>
      val attaching = items.setBy (_._1)
      if (items.isEmpty)
        throw new ControllerException ("Must list at least one file or device to attach.")
      if (attaching.size != items.size)
        throw new ControllerException ("Cannot attach a path multiple times.")
      items foreach (_._3.validForConfig())
      attachreqs = attachreqs.enqueue (items, cb)
    }}

  def attach (items: Seq [DriveAttachment]): Async [Unit] =
    guard {
      val files =
        for (item <- items)
          yield (item.path, openFile (item.path, item.geometry), item.geometry)
      _attach (files)
    }

  private def _detach (items: List [DiskDrive]): Unit =
    queue.run (ignore) {

      val keepDrives = this.drives -- (items map (_.id))
      val keepPaths = keepDrives.values.setBy (_.path)
      val bootgen = this.bootgen + 1
      val newboot = BootBlock (sysid, bootgen, number, keepPaths)

      for {
        _ <- drives.values.latch (_.checkpoint (newboot, None))
        _ <- supply {
          this.drives = keepDrives
          this.bootgen = bootgen
        }
        _ <- items.latch (_.detach())
      } yield {
        log.detachedDrives (items.setBy (_.path))
      }}

  def detach (disk: DiskDrive): Unit =
    queue.execute {
      detachreqs ::= disk
    }

  private def _drain (items: Seq [Path], cb: Callback [Unit]): Unit =
    queue.run (cb) {

      val byPath = drives.values.mapBy (_.path)

      if (!(items forall (byPath contains _))) {
        val unattached = (items.toSet -- byPath.keySet).toSeq.sorted
        throw new ControllerException (s"No such disks are attached: ${unattached mkString ", "}")
      }

      val drainDrives = items .map (byPath.apply _) .filterNot (_.draining)
      val drainPaths = drainDrives.setBy (_.path)

      if (drainDrives.size == drives.values.filterNot (_.draining) .size)
        throw new ControllerException ("Cannot drain all disks.")

      for {
        segs <- drainDrives.latch.collect (_.drain())
      } yield {
        checkpointer.checkpoint()
            .ensure (compactor.drain (segs.flatten))
            .run (ignore)
        log.drainingDrives (drainPaths)
      }}

  def drain (items: Seq [Path]): Async [Unit] =
    queue.async { cb =>
      if (items.isEmpty)
        throw new ControllerException ("Must list at least one file or device to drain.")
      if (items.size != items.toSet.size)
        throw new ControllerException ("Cannot drain a file or device twice.")
      drainreqs = drainreqs.enqueue (items, cb)
    }

  private def _checkpoint (marks: Map [Int, Long], cb: Callback [Unit]): Unit =
    queue.run (cb) {
      val bootgen = this.bootgen + 1
      val attached = drives.values.setBy (_.path)
      val newBoot = BootBlock (sysid, bootgen, number, attached)
      for {
        _ <- drives.values.latch (disk => disk.checkpoint (newBoot, marks get disk.id))
      } yield {
        this.bootgen = bootgen
      }}

  def checkpoint (marks: Map [Int, Long]): Async [Unit] =
    queue.async { cb =>
      checkreqs = checkreqs.enqueue (marks, cb)
    }

  def add (drives: Seq [DiskDrive]): Async [Unit] =
    fiber.supply {
      this.drives ++= drives.mapBy (_.id)
      drives foreach (_.added())
    }

  def mark(): Async [Map [Int, Long]] =
    guard {
      drives.values.latch.collate (_.mark())
    }

  def cleanable(): Async [Iterable [SegmentPointer]] =  {
    guard {
      for {
        segs <- drives.values.filterNot (_.draining) .latch.collect (_.cleanable())
      } yield segs.flatten
    }}

def fetch [P] (desc: PageDescriptor [P], pos: Position): Async [P] =
  guard {
    val drive = drives (pos.disk)
    DiskDrive.read (drive.file, drive.geom, desc, pos)
  }}
