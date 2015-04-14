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

import java.nio.file.Path

import com.treode.async.Async, Async.{async, supply}
import com.treode.disk.{Disk, DiskConfig, DiskController, DiskSystemDigest, DriveAttachment,
  DriveChange, DriveDigest, DriveGeometry, ObjectId, OversizedPageException,
  OversizedRecordException, PageDescriptor, PickledPage, Position, RecordDescriptor}
import com.treode.notify.Notification

/** The live Disk system. Implements the user and admin traits, Disk and DiskController, by
  * delegating to the appropriate components.
  */
private class DiskAgent (
  logdsp: LogDispatcher,
  pagdsp: PageDispatcher,
  group: DriveGroup
) (
  implicit config: DiskConfig
) extends Disk with DiskController {

  import config.{maximumPageBytes, maximumRecordBytes}

  implicit val disk = this

  /** Called by the LaunchAgent when launch completes. */
  def launch(): Unit =
    group.launch()

  def record [R] (desc: RecordDescriptor [R], record: R): Async [Unit] =
    async { cb =>
      val p = PickledRecord (desc, record, cb)
      if (p.byteSize > maximumRecordBytes)
        throw new OversizedRecordException (maximumRecordBytes, p.byteSize)
      logdsp.send (p)
    }

  def read [P] (desc: PageDescriptor [P], pos: Position): Async [P] =
    for {
      buf <- group.read (pos)
    } yield {
      desc.ppag.unpickle (buf)
    }

  def write [P] (desc: PageDescriptor [P], obj: ObjectId, gen: Long, page: P): Async [Position] =
    async { cb =>
      val p = PickledPage (desc, obj, gen, page, cb)
      if (p.byteSize > maximumPageBytes)
        throw new OversizedPageException (maximumPageBytes, p.byteSize)
      pagdsp.send (p)
    }

  def change (change: DriveChange): Async [Notification] =
    group.change (change)

  def attach (attaches: DriveAttachment*): Async [Notification] =
    change (DriveChange (attaches, Seq.empty))

  def attach (path: Path, geom: DriveGeometry): Async [Notification] =
    attach (DriveAttachment (path, geom))

  def drain (drains: Path*): Async [Notification] =
    change (DriveChange (Seq.empty, drains))

  /** Schedule a checkpoint. Normally logging thresholds trigger a checkpooint; this method allows
    * test to explicitly trigger ones.
    */
  def checkpoint(): Async [Unit] =
    supply (())

  def digest: Async [DiskSystemDigest] =
    for {
      driveDigests <- group.digests
    } yield {
      new DiskSystemDigest (driveDigests)
    }

  def shutdown(): Async [Unit] =
    group.close()

  // TODO
  def compact (desc: PageDescriptor [_], obj: ObjectId): Unit = ???
  def release (desc: PageDescriptor [_], obj: ObjectId, gens: Set [Long]): Unit = ???
  def join [A] (task:  Async[A]): Async[A] = ???
  def drives: Async [Seq [DriveDigest]] = ???
}
