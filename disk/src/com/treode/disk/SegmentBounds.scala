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

private case class SegmentBounds (num: Int, base: Long, limit: Long) {

  def contains (pos: Long): Boolean =
    base <= pos && pos < limit
}

private object SegmentBounds {

  def apply (num: Int, geom: DriveGeometry, config: DiskConfig): SegmentBounds = {
    require (0 <= num && num < geom.segmentCount)
    val start = num.toLong << geom.segmentBits
    val end = (num.toLong + 1) << geom.segmentBits
    val pos = if (start < geom.blockBytes) geom.blockBytes else start
    val limit = if (end > geom.diskBytes) geom.diskBytes else end
    assert (pos < limit)
    SegmentBounds (num, pos, limit)
  }}
