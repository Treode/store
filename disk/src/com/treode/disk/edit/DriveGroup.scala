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

package com.treode.disk.edit

import java.nio.file.{Path, StandardOpenOption}, StandardOpenOption._
import java.util.{ArrayDeque, ArrayList}
import scala.collection.SortedSet
import scala.util.{Failure, Success}

import com.treode.async.{Async, AsyncQueue, Callback, Fiber, Scheduler}, Async.guard, Callback.ignore
import com.treode.async.implicits._
import com.treode.async.misc.RichOption
import com.treode.buffer.PagedBuffer
import com.treode.disk.{Disk, DiskConfig, DisksClosedException, DiskEvents, DriveChange, DriveDigest,
  DriveGeometry, FileSystem, ObjectId, Position, SegmentBounds, TypeId, quote}

import DriveGroup._

/** All the disk drives in the disk system.
  *
  * The superblock stores the set of paths attached to this disk system, so every change to the set
  * requires a write to the superblock. The DriveGroup ensures there is only one active write to
  * superblock at a time. If changes arrive while the superblock is being written, then they are
  * queued until the superblock is completely written.
  *
  * @param drives
  * The initial set of disk drives. Null after the disk system has closed.
  *
  * @param gen
  * The current generation of the superblock. We increment this everytime we write the superblock.
  *
  * @param dno
  * The next ID for the next new disk drive. We increment this evertime we add a new disk drive,
  * and the new drive, so we never reuse drive IDs.
  */
private class DriveGroup (
  private var drives: Map [Int, Drive],
  private var gen: Int,
  private var dno: Int
) (implicit
  files: FileSystem,
  scheduler: Scheduler,
  config: DiskConfig,
  events: DiskEvents
) {

  private val fiber = new Fiber
  private val queue = new AsyncQueue (fiber) (reengage _)
  private var state: State = Opening

  /** New drives that are queued to attach. */
  private var attaches = List.empty [Drive]

  /** Attached drives that are queued to start draining. */
  private var drains = List.empty [Drive]

  /** Attached drives that are drained and queued to detach. */
  private var detaches = List.empty [Drive]

  /** Callbacks for when the next superblock is written. */
  private var changers = List.empty [Callback [Unit]]

  /** Callback for when the next superblock is written, and this completes a checkpoint. */
  private var checkpoint = Option.empty [Callback [Unit]]

  /** Callbacks for when the files are closed. */
  private var closing = List.empty [Callback [Unit]]

  drives = drives.withDefault (id => throw new IllegalArgumentException (s"Drive $id does not exist"))

  queue.launch()

  private val driveDrainStarted: Callback [Drive] = {
    case Success (drive) => detach (drive)
    case Failure (t) => throw t
  }

  private def requireNotClosed(): Unit =
    if (state == Closed)
      throw new DisksClosedException

  private def reengage() {
    if (state != Open)
      ()
    else if (!closing.isEmpty)
      _close()
    else if (!changers.isEmpty || !detaches.isEmpty || !checkpoint.isEmpty)
      _writeSuperblock()
  }

  /** Close the disks now, drop all queued changes, fail all queued callbacks. */
  private def _close() {

    state = Closed

    val drives = this.drives
    this.drives = null
    val attaches = this.attaches
    this.attaches = List.empty
    this.drains = List.empty
    val detaches = this.detaches
    this.detaches = List.empty
    val changes = this.changers
    changers = List.empty
    val checkpoint = this.checkpoint
    this.checkpoint = Option.empty

    for (drive <- drives.values)
      drive.close()
    for (drive <- attaches)
      drive.close()
    for (cb <- changers)
      scheduler.fail (cb, new DisksClosedException)
    for (cb <- checkpoint)
      scheduler.fail (cb, new DisksClosedException)
    for (cb <- closing)
      scheduler.pass (cb, ())
  }

  /** Process all queued changes, write the new superblock, pass all queued callbacks. */
  private def _writeSuperblock(): Unit =
    queue.begin {

      val attaches = this.attaches
      this.attaches = List.empty
      val drains = this.drains
      this.drains = List.empty
      val detaches = this.detaches
      this.detaches = List.empty
      val changers = this.changers
      this.changers = List.empty
      val checkpoint = this.checkpoint
      this.checkpoint = Option.empty

      drives = drives -- (detaches map (_.id))
      drives ++= (for (d <- attaches) yield (d.id, d))
      gen += 1

      val paths = (for (drive <- drives.values) yield drive.path).toSet
      val common = SuperBlock.Common (gen, dno, paths)

      for {
        _ <- drains.latch (_.startDraining())
        _ <- drives.latch (_._2.writeSuperblock (common))
      } yield {

        for (drive <- attaches)
          drive.launch()
        for (drive <- detaches)
          drive.close()
        for (drive <- drains)
          drive.awaitDrainStarted() run (driveDrainStarted)
        for (cb <- changers)
          scheduler.pass (cb, ())
        for (cb <- checkpoint)
          scheduler.pass (cb, ())

        events.changedDisks (
          attached = SortedSet.empty [Path] ++ attaches.map (_.path),
          detached = SortedSet.empty [Path] ++ detaches.map (_.path),
          draining = SortedSet.empty [Path] ++ drains.map (_.path))
      }}

  def launch (): Unit =
    fiber.execute {
      state = Open
      for (drive <- drives.values)
        drive.launch()
      queue.engage()
    }

  def startCheckpoint(): Async [Unit] =
    fiber.supply {
      assert (state != Checkpointing && checkpoint.isEmpty)
      state = Checkpointing
    }

  def finishCheckpoint(): Async [Unit] =
    fiber.async { cb =>
      assert (state == Checkpointing && checkpoint.isEmpty)
      state = Open
      checkpoint = Some (cb)
      queue.engage()
    }

  /** Enqueue user changes to the set of disk drives. */
  def change (change: DriveChange): Async [Unit] =
    fiber.async { cb =>
      requireNotClosed()

      // The paths that are already attached.
      val attached = (for (d <- drives.values) yield d.path).toSet

      // The paths already queued to be attached.
      var attaching = (for (d <- attaches) yield d.path).toSet

      // Our view of the next disk ID.
      var dno = this.dno

      // Drives we will queue to attach; we build this up below.
      var newAttaches = List.empty [Drive]

      // Process each of the attaches:
      // - check that the path is not alredy attached or queued to be attached,
      // - open the file,
      // - assign it a drive ID,
      // - make the drive, and
      // - add it to the list of new attaches.
      for (a <- change.attaches) {
        if (attached contains a.path)
          throw new IllegalArgumentException (s"Already attached ${quote (a.path)}")
        if (attaching contains a.path)
          throw new IllegalArgumentException (s"Already attaching ${quote (a.path)}")
        attaching += a.path
        val file = files.open (a.path, READ, WRITE)
        val drive = new Drive (file, a.geometry, false, dno, a.path)
        dno += 1
        newAttaches ::= drive
      }

      // The paths that are already queued draining.
      var draining = (for (d <- drains) yield d.path).toSet

      // Drives we will queue to drain; we build this up below.
      var newDrains = List.empty [Drive]

      // Process each of the drains:
      // - check that the path is not already draining or queued to start draining, and
      // - add it to the list of new drains.
      for (d <- change.drains) {
        val drive = drives.values.find (_.path == d) match {
          case Some (drive) => drive
          case None => throw new IllegalArgumentException (s"Drive ${quote (d)} not attached")
        }
        if (drive.draining || (draining contains d))
          throw new IllegalArgumentException (s"Aready draining ${quote (d)}")
        drains ::= drive
      }

      // If we get here, then all went well, so we can merge our new changes into the queued
      // changes.
      this.dno = dno
      attaches :::= newAttaches
      drains :::= newDrains
      changers ::= cb
      queue.engage()
    }

  /** Enqueue internal changes to the set of disk drives. */
  def detach (drive: Drive): Unit =
    fiber.execute {
      detaches ::= drive
      queue.engage()
    }

  def digests: Async [Seq [DriveDigest]] =
    fiber.supply {
      requireNotClosed()
      drives.map (_._2.digest) .toSeq
    }

  def close(): Async [Unit] =
    fiber.async { cb =>
      if (drives == null)
        scheduler.pass (cb, ())
      else
        closing ::= cb
    }}

private object DriveGroup {

  sealed abstract class State
  case object Opening extends State
  case object Open extends State
  case object Checkpointing extends State
  case object Closed extends State
}