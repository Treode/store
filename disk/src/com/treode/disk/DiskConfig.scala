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

/** Configuration parameters for the [[Disk]] system.
  *
  * @param checkpointBytes
  * Begin a checkpoint after this many bytes have been logged.
  *
  * @param  checkpointEntries
  * Begin a checkpoint after this many entries have been logged.
  *
  * @param maximumRecordBytes
  * Reject log records larger than this limit.
  *
  * @param maximumPageBytes
  * Reject pages larger than this limit.
  *
  * @param pageCacheEntries
  * Cache this many pages.
  *
  * @param superBlockBits
  * The size of the superblock in bits (for example, 14 = 16K).
  */
case class DiskConfig (
    checkpointBytes: Int,
    checkpointEntries: Int,
    maximumRecordBytes: Int,
    maximumPageBytes: Int,
    pageCacheEntries: Int,
    superBlockBits: Int
) {

  require (
      checkpointBytes > 0,
      "The checkpoint interval must be more than 0 bytes.")

  require (
      checkpointEntries > 0,
      "The checkpoint interval must be more than 0 entries.")

  require (
      maximumRecordBytes > 0,
      "The maximum record size must be more than 0 bytes.")

  require (
      maximumPageBytes > 0,
      "The maximum page size must be more than 0 bytes.")

  require (
      pageCacheEntries > 0,
      "The size of the page cache must be more than 0 entries.")

  require (
      superBlockBits > 0,
      "A superblock must have more than 0 bytes.")

  val superBlockBytes = 1 << superBlockBits
  val superBlockMask = superBlockBytes - 1
  val diskLeadBytes = 1 << (superBlockBits + 1)

  val minimumSegmentBits = {
    val bytes = math.max (maximumRecordBytes, maximumPageBytes)
    Integer.SIZE - Integer.numberOfLeadingZeros (bytes - 1) + 1
  }

  def checkpoint (bytes: Long, entries: Long): Boolean =
    bytes > checkpointBytes.toLong || entries > checkpointEntries.toLong
}

object DiskConfig {

  def suggested (
    checkpointBytes: Int = 1 << 24,
    checkpointEntries: Int = 10000,
    maximumRecordBytes: Int = 1 << 24,
    maximumPageBytes: Int = 1 << 24,
    pageCacheEntries: Int = 10000,
    superBlockBits: Int = 14
  ): DiskConfig =
    new DiskConfig (
      checkpointBytes,
      checkpointEntries,
      maximumRecordBytes,
      maximumPageBytes,
      pageCacheEntries,
      superBlockBits)
}