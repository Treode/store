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

import Allocator.segmentBounds

private class Allocator private (private var _free: IntSet) {

  def alloc (geometry: DriveGeometry, config: Disk.Config): SegmentBounds = {
    val iter = _free.iterator
    if (!iter.hasNext)
      throw new DiskFullException
    val num = iter.next
    _free = _free.remove (num)
    segmentBounds (num, geometry, config)
  }

  def alloc (num: Int, geometry: DriveGeometry, config: Disk.Config): (SegmentBounds, Boolean) = {
    // Reporting if we alloc the same num more than once
    var alreadyAlloced = !_free.contains(num)
    _free = _free.remove (num)
    (segmentBounds (num, geometry, config), alreadyAlloced)
  }

  def free (nums: IntSet): Unit =
    _free = _free.add (nums)

  def free: IntSet = _free

  def cleanable (ignore: IntSet): IntSet =
    free.complement.remove (ignore)

  def drained (ignore: IntSet): Boolean = {
    val alloc = free.complement.remove (ignore)
    alloc.size == 0
  }}

private object Allocator {

  def segmentBounds (num: Int, geometry: DriveGeometry, config: Disk.Config): SegmentBounds = {
    require (0 <= num && num < geometry.segmentCount)
    val pos = if (num == 0) config.diskLeadBytes else num.toLong << geometry.segmentBits
    val end = (num.toLong + 1) << geometry.segmentBits
    val limit = if (end > geometry.diskBytes) geometry.diskBytes else end
    SegmentBounds (num, pos, limit)
  }

  def apply (free: IntSet): Allocator =
    new Allocator (free)

  def apply (geometry: DriveGeometry, config: Disk.Config): Allocator = {
    val all = IntSet.fill (geometry.segmentCount)
    val superbs = IntSet.fill (config.diskLeadBytes >> geometry.segmentBits)
    new Allocator (all.remove (superbs))
  }}
