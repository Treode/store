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

  /** See [[Disk#read Disk.read]]. */
  def read (pos: Position) (implicit disk: Disk): Async [P] =
    disk.read (this, pos)

  /** See [[Disk#write Disk.write]]. */
  def write (obj: ObjectId, gen: Long, page: P) (implicit disk: Disk): Async [Position] =
    disk.write (this, obj, gen, page)

  /** See [[Disk#compact Disk.compact]]. */
  def compact (obj: ObjectId) (implicit disk: Disk): Unit =
    disk.compact (this, obj)

  /** Deprecated. */
  def handle (handler: PageHandler) (implicit launch: DiskLaunch): Unit =
    launch.handle (this, handler)

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
