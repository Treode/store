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

class DriveGeometry private (
    val segmentBits: Int,
    val blockBits: Int,
    val diskBytes: Long
) {

  val segmentBytes = 1 << segmentBits
  val segmentMask = ~(segmentBytes - 1)

  val blockBytes = 1 << blockBits
  val blockMask = ~(blockBytes - 1)

  val segmentCount = ((diskBytes + segmentBytes - (blockBytes<<2)) >> segmentBits).toInt

  def blockAlignDown (pos: Int): Int =
    pos & blockMask

  def blockAlignDown (pos: Long): Long =
    pos & blockMask.toLong

  def blockAlignUp (length: Int): Int =
    (length + blockBytes - 1) & blockMask

  def blockAlignUp (length: Long): Long =
    (length + blockBytes - 1) & blockMask.toLong

  private [disk] def segmentNum (pos: Long): Int =
    (pos >> segmentBits) .toInt

  private [disk] def segmentBounds (num: Int) (implicit config: DiskConfig): SegmentBounds = {
    require (0 <= num && num < segmentCount)
    val pos = if (num == 0) config.diskLeadBytes else num.toLong << segmentBits
    val end = (num.toLong + 1) << segmentBits
    val limit = if (end > diskBytes) diskBytes else end
    SegmentBounds (num, pos, limit)
  }

  private [disk] def validForConfig() (implicit config: DiskConfig) {
    require (
        blockBits <= config.superBlockBits,
        "A superblock must be at least one disk block.")
    require (
        segmentBits >= config.minimumSegmentBits,
        "A segment must be larger than the largest record or page.")
  }

  override def hashCode: Int =
    (segmentBits, blockBits, diskBytes).hashCode

  override def equals (other: Any): Boolean =
    other match {
      case that: DriveGeometry =>
        segmentBits == that.segmentBits &&
        blockBits == that.blockBits &&
        diskBytes == that.diskBytes
      case _ =>
        false
    }

  override def toString: String =
    s"DriveGeometry($segmentBits, $blockBits, $diskBytes)"
}

object DriveGeometry {

  def apply (
      segmentBits: Int,
      blockBits: Int,
      diskBytes: Long
  ): DriveGeometry = {

    require (
        segmentBits > 0,
        "A segment must have more than 2^0 bytes.")
    require (
        blockBits > 0,
        "A block must have more than 2^0 bytes.")
    require (
        diskBytes > 0,
        "A disk must have more than 0 bytes.")
    require (
        segmentBits >= blockBits,
        "A segment must be at least one block.")
    require (
        diskBytes >> segmentBits >= 16,
        "A disk must have at least 16 segments")

    new DriveGeometry (
        segmentBits,
        blockBits,
        diskBytes)
  }

  def standard (
      segmentBits: Int = 30,
      blockBits: Int = 13,
      diskBytes: Long = -1
  ): DriveGeometry =
     DriveGeometry (
         segmentBits,
         blockBits,
         diskBytes)

  val pickler = {
    import DiskPicklers._
    wrap (uint, uint, ulong)
    .build { v =>
      val (s, b, d) = v
      require (s > 0 && b > 0 && d > 0)
      new DriveGeometry (s, b, d)
    }
    .inspect (v => (v.segmentBits, v.blockBits, v.diskBytes))
  }}
