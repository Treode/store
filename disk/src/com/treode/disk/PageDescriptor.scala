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

import scala.reflect.ClassTag
import com.treode.async.Async
import com.treode.pickle.Pickler

/** Describes a page of data. */
class PageDescriptor [P] private (

    /** The ID to tag pages in the segment map; see [[PageHandler]]. */
    val id: TypeId,

    /** The pickler to serialize the page data. */
    val ppag: Pickler [P]
) (
    implicit val tpag: ClassTag [P]
) {

  /** Register a handler to work with the cleaner.
    *
    * A handler must be registered with the launch builder. The disk system will panic if it
    * discovers a page of data that it cannot identify.
    */
  def handle (handler: PageHandler) (implicit launch: Disk.Launch): Unit =
    launch.handle (this, handler)

  /** Read a page.
    *
    * @param pos The position to read from.
    */
  def read (pos: Position) (implicit disk: Disk): Async [P] =
    disk.read (this, pos)

  /** Write a page.
    *
    * @param obj The ID of the object; see [[PageHandler]].
    * @param grp The ID of the group; see [[PageHandler]].
    * @param page The data.
    */
  def write (obj: ObjectId, group: GroupId, page: P) (implicit disk: Disk): Async [Position] =
    disk.write (this, obj, group, page)

  /** Schedule the object for compaction.
    *
    * An object on disk should never spontaneously compact itself. If the object on disk desires
    * to be compacted, it should invoke this method. The disk system will eventually invoke the
    * compact method of the [[PageHandler]]. This achieves two things: the disk system can control
    * the number of compactions that are in flight at one time, and the disk system can double up
    * compaction desired by the object itself with compaction desired by the cleaner.
    */
  def compact (obj: ObjectId) (implicit disk: Disk): Async [Unit] =
    disk.compact (this, obj)

  override def toString = s"PageDescriptor($id)"
}

object PageDescriptor {

  def apply [P] (
    id: TypeId,
    ppag: Pickler [P]
  ) (implicit
    tpag: ClassTag [P]
  ): PageDescriptor [P] =
    new PageDescriptor (id, ppag)
}
