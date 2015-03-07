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

import com.treode.async.Async
import com.treode.pickle.Pickler

/** Describes an entry in the write log. */
class RecordDescriptor [R] private (

  /** The ID to tags the entry on disk. */
  val id: TypeId,

  /** The pickler to serialize the entry. */
  val prec: Pickler [R]) {

  /** Record an entry into the write log.
    *
    * '''Durability''': When the async completes, the entry has reached disk, and it will be
    * replayed during system recovery. However, checkpoints clean old entries from the log.
    *
    * '''Checkpoints''': Most entries logged before the most recent checkpoint will not be
    * replayed, but a few may. All entries logged after the most recent checkpoint will be
    * replayed.
    *
    * '''Registration''': A replayer must be registered with the recovery builder. The disk system
    * will refuse to recover if it cannot identify all log entries.
    *
    * '''Ordering''': When the async of a first log entry has completed before record has been
    * invoked with a second log entry, the first will be replayed before the second. When the
    * async of a first log entry has yet to complete and record is invoked with a second, the
    * second may be replayed before the first.
    *
    * @param entry The entry to record.
    */
  def record (entry: R) (implicit disk: Disk): Async [Unit] =
    disk.record (this, entry)

  /** Register a replayer with the recovery builder.
    *
    * A replayer must be registered with the recovery builder. The disk system will refuse to
    * recover if it cannot identify all log entries.
    */
  def replay (f: R => Any) (implicit recovery: DiskRecovery): Unit =
    recovery.replay (this) (f)

  override def toString = s"RecordDescriptor($id)"
}

object RecordDescriptor {

  def apply [R] (id: TypeId, prec: Pickler [R]): RecordDescriptor [R] =
    new RecordDescriptor (id, prec)
}
