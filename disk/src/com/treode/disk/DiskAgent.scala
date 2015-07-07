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

import com.treode.async.{Async, Scheduler}, Async.async
import com.treode.async.misc.EpochReleaser
import com.treode.notify.Notification

/** The live Disk system. Implements the user and admin traits, Disk and DiskController, by
  * delegating to the appropriate components.
  */
private class DiskAgent (
  logdsp: LogDispatcher,
  pagdsp: PageDispatcher,
  compactor: Compactor,
  releaser: EpochReleaser,
  ledger: SegmentLedger,
  group: DriveGroup,
  cache: PageCache
) extends Disk with DiskController {

  implicit val disk = this

  /** Called by the LaunchAgent when log replay completes. */
  def recover (writers: Map [Int, Long]): Unit =
    group.recover (writers)

  /** Called by the LaunchAgent when launch completes. */
  def launch (checkpoints: CheckpointerRegistry, compactors: CompactorRegistry): Unit =
    group.launch (checkpoints, compactors)

  def record [R] (desc: RecordDescriptor [R], record: R): Async [Unit] =
    logdsp.record (desc, record)

  def read [P] (desc: PageDescriptor [P], pos: Position): Async [P] =
    cache.read (desc, pos)

  def write [P] (desc: PageDescriptor [P], obj: ObjectId, gen: Long, page: P): Async [Position] =
    for {
      pos <- pagdsp.write (desc, obj, gen, page)
    } yield {
      cache.write (pos, page)
      pos
    }

  def compact (desc: PageDescriptor[_], obj: ObjectId): Unit =
    compactor.compact (desc.id, obj)

  def release (desc: PageDescriptor[_], obj: ObjectId, gens: Set [Long]): Unit = {
    val docket = ledger.free (desc.id, obj, gens)
    docket.remove (group.protect)
    releaser.release (group.release (docket))
  }

  def join [A] (task:  Async[A]): Async[A] =
    releaser.join (task)

  def change (change: DriveChange): Async [Notification [Unit]] =
    group.change (change)

  /** Bypass cache; for testing. */
  def fetch [P] (desc: PageDescriptor [P], pos: Position): Async [P] =
    group.fetch (desc, pos)

  /** Force a checkpoint; for testing. */
  def checkpoint(): Async [Unit] =
    group.checkpointer.checkpoint()

  def digest: Async [DiskSystemDigest] =
    for {
      driveDigests <- group.digests
    } yield {
      new DiskSystemDigest (driveDigests)
    }

  def shutdown(): Async [Unit] =
    group.close()
}
