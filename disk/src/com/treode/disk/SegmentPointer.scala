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

private class SegmentPointer private (
    val disk: DiskDrive,
    val bounds: SegmentBounds
) {

  def num = bounds.num
  def base = bounds.base
  def limit = bounds.limit

  def compacting(): Unit =
    disk.compacting (Seq (this))

  def probe(): Async [PageLedger] =
    PageLedger.read (disk.file, disk.geom, bounds.base)

  def free(): Unit =
    disk.free (this)

  override def equals (other: Any): Boolean =
    other match {
      case that: SegmentPointer =>
        (disk.id, bounds.num) == (that.disk.id, that.bounds.num)
      case _ => false
    }

  override def hashCode: Int =
    (disk.id, bounds.num).hashCode

  override def toString: String =
    s"SegmentPointer(${disk.id}, ${bounds.num})"
}

private object SegmentPointer {

  def apply (disk: DiskDrive, bounds: SegmentBounds): SegmentPointer =
    new SegmentPointer (disk, bounds)
}
