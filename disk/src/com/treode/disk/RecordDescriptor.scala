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

  /** See [[Disk#record Disk.record]]. */
  def record (entry: R) (implicit disk: Disk): Async [Unit] =
    disk.record (this, entry)

  /** See [[DiskRecovery#replay DiskRecovery.replay]]. */
  def replay (f: R => Any) (implicit recovery: DiskRecovery): Unit =
    recovery.replay (this) (f)

  override def toString = s"RecordDescriptor($id)"
}

object RecordDescriptor {

  def apply [R] (id: TypeId, prec: Pickler [R]): RecordDescriptor [R] =
    new RecordDescriptor (id, prec)
}
