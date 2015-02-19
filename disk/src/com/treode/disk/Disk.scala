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
import com.treode.async.{Async, Callback, Scheduler}

/** The Disk System.
  *
  * The disk system provides a write log and write-once pages. It runs checkpoints to reclaim log
  * space. It runs a segment cleaner to compact useful pages and collect garbage.
  *
  * ## Overview
  *
  * The log provides a fast mechanism to record short entries on disk. The pages provide a
  * mechanism to save larger items. You use the two together to implement a persistent store.
  *
  * Consider a table with get and put. The table can keep the most recent values in memory. When a
  * new value is put, the table can record it to memory and to the log. When recovering the system,
  * the log will be replayed. If the log were never truncated, the table could recover its entire
  * memory image during log replay. However, disks have limits and the log cannot grow without
  * bound.
  *
  * When the log gets long, the disk system will run a checkpoint before dropping old log entries.
  * At that time, the table should write a snapshot of its memory image to a page. The table can
  * partially rebuild its image during replay. After that, the table can fetch the most recent
  * snapshot from the page, then it can apply the partial rebuild. Now the memory image has been
  * recovered. The table must register a checkpont method for this process.
  *
  * After many pages have been written, the cleaner will probe the table to learn which pages are
  * in use. It will then decide to reclaim disk space. That space may contain some garbage pages
  * and some live pages. The cleaner will ask the table to compact its live pages, at which time
  * the table must rewrite them to a new location. The table must register a [[PageHandler]] for
  * this process.
  *
  * That is, broadly speaking, how a persistent store interacts with the log, its checkpointer,
  * the write-once pages, and their cleaner.
  *
  * ## Recovery
  *
  * Disk recovery follows something a bit like the builder pattern, however it proceeds in phases,
  * so there are multiple builders. In the recovery phase, you first register log replayers, and
  * then reattach one or more disks. The system replays the logs from those disks, and then it
  * yields a launcher. In the launch phase you register checkpointers and page handlers, and then
  * launch the disk system. At that time, the system starts the checkpointer and cleaner.
  *
  * ## Allocation and Reclaimation
  *
  * The disk system divdes a disk drive into segments. The log writer and page writer each
  * allocate a segment for themselves. They write until the segment is full, and then request
  * another segment. The size of a segment is configured per disk; see [[DriveGeometry]].
  *
  * The checkpointer runs periodically; see [[Disk.Config]]. The checkpointer marks the current
  * log position, then invokes every registered checkpoint method, and then drops entries upto
  * the marked position. When the checkpointer has dropped all log entries in the segment, it frees
  * the segment.
  *
  * The segment cleaner runs periodically; see [[Disk.Config]]. It probes the pages of allocated
  * segments to estimate the number of live bytes in each. It chooses the segments with the least
  * number of live bytes and compacts the live pages on them, which copies the live data to
  * someplace else. Finally, it the frees the segment. The cleaner uses registered [[PageHandler]]s
  * to assist probing and compacting.
  *
  * Readers may have obtain pointers to live pages before the compactor begins, and they may hold
  * those pointers while they data is copied elsewhere. However, those readers remain unaware of
  * the relocation. To prevent the compactor from freeing the formerly live pages while readers
  * still hold pointers, the readers join a release epoch, and they leave that epoch when they
  * are done. The compactor adds reclaimation tasks to the release epoch, and those tasks are
  * delayed until all readers have left the epoch.
  *
  * ## Usage
  *
  * To work with the write log, you use a descriptor to write entries, and to register a replayer
  * during recovery. You must also register a checkpoint method during launch. The checkpoint
  * method must persist data to pages so that log entries can be dropped.
  *
  * To work with the write-once pages, you use a descriptor to write a page, and to read it
  * afterward. You must register a page handler during launch. The page handler must estimate live
  * bytes, and it must compact pages when requested.
  */
trait Disk {

  /** See [[RecordDescriptor#record]]. */
  def record [R] (desc: RecordDescriptor [R], entry: R): Async [Unit]

  /** See [[PageDescriptor#read]]. */
  def read [P] (desc: PageDescriptor [P], pos: Position): Async [P]

  /** See [[PageDescriptor#write]]. */
  def write [P] (desc: PageDescriptor [P], obj: ObjectId, group: GroupId, page: P): Async [Position]

  /** See [[PageDescriptor#compact]]. */
  def compact (desc: PageDescriptor [_], obj: ObjectId): Async [Unit]

  /** Join a release epoch, and leave it when the async task completes. Pages which are live at the
    * beginning of the epoch will remain available, even if they should become unreachable during
    * the epoch. The cleaner may discover that pages are unreachable during the epoch, but will not
    * release them until every party that joined the epoch has left it.
    *
    * The task may directly have disk positiions ([[Position]]), for example in a local variable of
    * a method or in a member field of an object. It may also indirectly have disk positions, for
    * example it may directly have the position of the root of an index tree. All these pages are
    * effectively reachable from the task. While a part of the release epoch, the task may safely
    * read all pages that is has directly and indirectly. They will not be reclaimed and
    * overwritten until the task as left the epoch.
    */
  def join [A] (task: Async [A]): Async [A]
}

object Disk {

  case class Config (

      /** Begin a checkpoint after this many bytes have been logged. */
      checkpointBytes: Int,

      /** Begin a checkpoint after this many entries have been logged. */
      checkpointEntries: Int,

      /** Probe and compact after this many segments have been allocated. */
      cleaningFrequency: Int,

      /** Compact this many segments at a time. */
      cleaningLoad: Int,

      /** Reject log records larger than this limit. */
      maximumRecordBytes: Int,

      /** Reject pages larger than this limit. */
      maximumPageBytes: Int,

      /** Cache this many pages. */

      pageCacheEntries: Int,

      /** The size of the superblock in bits (for example, 14 = 16K). */
      superBlockBits: Int
  ) {

    require (
        checkpointBytes > 0,
        "The checkpoint interval must be more than 0 bytes.")

    require (
        checkpointEntries > 0,
        "The checkpoint interval must be more than 0 entries.")

    require (
        cleaningFrequency > 0,
        "The cleaning interval must be more than 0 segments.")

    require (
        cleaningLoad > 0,
        "The cleaning load must be more than 0 segemnts.")

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

    def checkpoint (bytes: Int, entries: Int): Boolean =
      bytes > checkpointBytes || entries > checkpointEntries

    def clean (segments: Int): Boolean =
      segments >= cleaningFrequency
  }

  object Config {

    val suggested = Config (
        checkpointBytes = 1 << 24,
        checkpointEntries = 10000,
        cleaningFrequency = 7,
        cleaningLoad = 1,
        maximumRecordBytes = 1 << 24,
        maximumPageBytes = 1 << 24,
        pageCacheEntries = 10000,
        superBlockBits = 14)
  }

  /** The disk controller.
    *
    * All the admin-y things you can do with the disk system.
    */
  trait Controller {

    /** This disk system. */
    implicit def disk: Disk

    /** Summary information of the drives attached to this disk system. */
    def drives: Async [Seq [DriveDigest]]

    /** Attach new drives.
      *
      * When this method returns, the drives are a part of the disk system.
      */
    def attach (items: DriveAttachment*): Async [Unit]

    /** Drain attached drives.
      *
      * The disk system drains drives by copying all live data on them to someplace else. When this
      * method returns, the drain has begun, but it may not complete until later. When they have
      * drained, the disk system will detach the drives and log a message.
      */
    def drain (items: Path*): Async [Unit]

    def shutdown(): Async [Unit]
  }

  /** The launch builder. */
  trait Launch {

    /** The disk system.
      *
      * It is ready for recording log entries, and reading and writing pages. However, before
      * launch it is not checkpointing or cleaning pages. It is not reclaiming segments.
      */
    implicit def disk: Disk

    /** The disk controller. */
    implicit def controller: Controller

    /** This system ID found in the superblock. */
    def sysid: Array [Byte]

    /** Register a checkpointer. */
    def checkpoint (f: => Async [Unit])

    /** Register a page handler. */
    def handle (desc: PageDescriptor [_], handler: PageHandler)

    /** Launch the checkpointer and cleaner.
      *
      * Call `launch` after registering all checkpointers and page handlers. This method closes
      * this launch builder.
      */
    def launch()
  }

  /** The recovery builder. */
  trait Recovery {

    /** Register a replayer for a log entry. */
    def replay [R] (desc: RecordDescriptor [R]) (f: R => Any)

    /** Reattach one or more paths.
      *
      * You need provide only some of the paths previously attached to this disk system. The
      * recovery mechanim will find the complete list of paths in the superblock. If not for this
      * behavior, you would need to attach and drain disks, and simultaneously update the config
      * files or startup scripts. With this feature, you can
      *
      *   - Add a disk:
      *
      *       1. Attach the disk; see [[Controller#attach]].
      *
      *       2. Add this disk to config files or startup scripts.
      *
      *   - Drain a disk.
      *
      *       1. Remove the disk from config files or startup scripts.
      *
      *       2. Drain the disk; see [[Controller#drain]].
      *
      * Call `reattach` ''after'' registering all replayers. This method closes this recovery
      * builder.
      */
    def reattach (items: Path*): Async [Launch]
  }

  /** Initialize the disk system.
    *
    * Setup the disk system with one or more disks; all disks get the same geometry. This does
    * not yield a [[Disk]] system to use immediately. It only writes superblocks and metadata
    * for disk recovery. After initializing the disks, call [[#recover]].
    *
    * The superblock bits provided to this initialization must also be supplied to recovery.
    *
    * @param sysid A small identifier of your choosing.
    * @param superBlockBits The size of the superblock in bits (for example, 14 = 16K).
    * @param blockBits The size of a disk block in bits.
    * @param diskBytes The size of the disks in bytes.
    * @param paths The paths of raw devices or files.
    */
  def init (
      sysid: Array [Byte],
      superBlockBits: Int,
      segmentBits: Int,
      blockBits: Int,
      diskBytes: Long,
      paths: Path*
  ): Unit =
    DiskDrive.init (sysid, superBlockBits, segmentBits, blockBits, diskBytes, paths)

  /** Start recovery; create a recovery builder.
    *
    * The `superBlockBits` in the [[Disk.Config]] must match the `superBlockBits` given to
    * initialization.
    */
  def recover () (implicit scheduler: Scheduler, config: Disk.Config): Recovery =
    new RecoveryAgent
}
