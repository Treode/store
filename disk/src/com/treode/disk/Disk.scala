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

/** ==The Disk System==
  *
  * The disk system provides a write log and write-once pages. Records in the write log are
  * replayed at system restart, and pages can be read at anytime after restart. The disk system
  * runs checkpoints to reclaim log space, and it coordinates compactors to defragment and reclaim
  * page space.
  *
  * ==Overview==
  *
  * The log provides a fast mechanism to record short entries on disk. The pages provide a
  * mechanism to save larger items. You use the two together to implement a persistent store.
  *
  * Consider a table with get and put. Although we speak of ''tables'' here, documentation
  * throughout the rest of the disk system uses the more generic term ''object''. The table keeps
  * the most recent values in memory. When a new value is put, the table records it to memory and
  * to the log (see [[#record Disk.record]]). When restarting the system, the log will be replayed
  * so that the table can recover its values in memory (see
  * [[DiskRecovery#replay DiskRecovery.replay]]).
  *
  * The log cannot grow without bound. To truncate it, the disk system will run a checkpoint, and
  * then it will drop old log entries. Druing checkpoint, the table will write a snapshot of its
  * memory to a page (see [[DiskLaunch#checkpoint DiskLaunch.checkpoint]]), and record the page's
  * location to the log. To recover during a later log replay, the table will construct recent
  * values in memory and the position of the snapshot.
  *
  * The table will build many snapshots overtime. To find a key, it will need to scan them linearly
  * until it finds the snapshot that contains that key. To keep the list of snapshots short, the
  * table will want to periodically merge snapshots, write the merged data to new pages, and
  * release the old pages (see [[#release Disk.release]]). This process is called compaction.
  *
  * This disk system coordinates compactors so that only one runs at a time. The table may schedule
  * itself for compaction (see [[Disk#compact Disk.compact]]). The disk system may also schedule
  * compaction when it wants to defragment disk space. Either way, when the disk system is ready
  * to compact the table, it will invoke the registered compactor (see
  * [[DiskLaunch#compact DiskLaunch.compact]]).
  *
  * Individual pages are identified by their [[Position]]; groups of pages are identified by a
  * [[TypeId]], [[ObjectId]] and generation (long). The [[#write write method]] returns the
  * position at which the page was written, and the [[#read read method]] uses that to return the
  * page. The compactor does not identify individual pages; instead, it identifies groups of pages
  * that belong to a generation. A table will manage generations as it sees fit, but generally
  * each checkpoint will be a new generation, and each compaction will be a new generation.
  *
  * ==Recovery==
  *
  * Recovering the disk uses a two-phase builder pattern. To create an instance of the `Disk`
  * class
  *
  * 1. Create a [[DiskRecovery]] using [[Disk#recover Disk.recover]].
  *
  * 2. Register methods to replay log entries (see [[DiskRecovery#replay DiskRecovery.replay]]).
  *
  * 3. Reattach one or more disk drives (see [[DiskRecovery#reattach DiskRecovery.reattach]]). This
  * replays log entries, and then yields a [[DiskLaunch]].
  *
  * You must register all replayers before reattaching any disks. An instance of `Disk` is inside
  * `DiskLaunch`. At this time the disk system is ready to [[#record record]] log entries, and to
  * [[#read read]]) and [[#write write]] pages. It is not yet ready to checkpoint or compact
  * objects.
  *
  * 4. Register methods to checkpoint and compact objects (see
  * [[DiskLaunch#checkpoint DiskLaunch.checkpoint]] and [[DiskLaunch#compact DiskLaunch.compact]]).
  * Also, claim pages (see [[DiskLaunch#claim DiskLaunch.claim]]).
  *
  * Your objects must claim generations of pages; unclaimed generations will be reclaimed by the
  * disk system; that is, they will be reused and overwritten. Why this step? The object may be
  * in the middle of writing a generation when the system crashes. The disk system may have a
  * record of those writes, yet the object may not. This step exists to identify pages which the
  * disk system knows about, yet the user structures do not, because of a system crash.
  *
  * 5. Launch the disk system (see [[DiskLaunch#launch DiskLaunch.launch]]). The disk system will
  * now run checkpointers and compactors.
  *
  * You must complete this last step, or disk space will never be reclaimed.
  *
  * ==Allocation and Reclaimation==
  *
  * The disk system divdes a disk drive into segments. The log writer and page writer each
  * allocate a segment for themselves. They write until the segment is full, and then request
  * another segment. The size of a segment is configured per disk; see [[DriveGeometry]].
  *
  * The checkpointer runs periodically; see [[DiskConfig]]. The checkpointer marks the current
  * log position, then invokes every registered checkpoint method, and then drops entries upto
  * the marked position. When the checkpointer has dropped all log entries in the segment, it frees
  * the segment.
  *
  * The compactor runs periodically; see [[DiskConfig]]. It probes the pages of allocated segments
  * to estimate the number of live bytes in each. It chooses the segments with the least number of
  * live bytes and compacts the live pages on them; that copies the live data somewhere else.
  * As the objects compact themselves, they invoke [[#release release]]. When all pages on a
  * segment have been released, it will be scheduled for reclaimation.
  *
  * The segment will not be reclaimed, reused and overwritten immediately, since there may be
  * readers in the system that acquired pointers (`Position`) to pages in that segment before the
  * object compacted and released them. Readers join a release epoch (see [[#join Disk.join]]) to
  * prevent reclaimation of pages until they complete.
  */
trait Disk {

  /** Record an entry into the write log.
    *
    * '''Durability''': When the async completes, the entry has reached disk, and it will be
    * replayed during system recovery. However, checkpoints clean old entries from the log.
    *
    * '''Checkpoints''': Most entries logged before the most recent checkpoint will not be
    * replayed, but a few may. All entries logged after the most recent checkpoint will be
    * replayed. Typically, an object includes a generation number in the log entry; that number
    * allows the replayer to determine if log entry came before or after a checkpoint.
    *
    * '''Ordering''': When the async of a first log entry has completed before record has been
    * invoked with a second log entry, the first will be replayed before the second. When the
    * async of a first log entry has yet to complete and record is invoked with a second, the
    * second may be replayed before the first. Typically, an object uses locks to prevent
    * conflicting updates, so this semi-predictable replay order is sufficient.
    *
    * @param desc The descriptor for the record.
    * @param entry The entry to record.
    */
  def record [R] (desc: RecordDescriptor [R], entry: R): Async [Unit]

  /** Read a page.
    *
    * @param desc The descriptor for the page.
    * @param pos The position to read from.
    */
  def read [P] (desc: PageDescriptor [P], pos: Position): Async [P]

  /** Write a page.
    *
    * The disk system uses `(desc.id, obj, gen)` to track allocation, and to manage compaction.
    *
    * @param desc The descriptor for the page.
    * @param obj The ID of the object.
    * @param gen The generation of the page.
    * @param page The data.
    */
  def write [P] (desc: PageDescriptor [P], obj: ObjectId, gen: Long, page: P): Async [Position]

  /** Schedule an object for compaction.
    *
    * An object should not spontaneously compact itself; it should invoke this method. The disk
    * system will eventually invoke the compact method registered during launch (see
    * [[DiskLaunch#compact DiskLaunch.compact]]).  This achieves two things: the disk system can
    * control the number of compactions that are in flight at the same time, and the disk system
    * can combine compaction desired by the object with that desired by the disk system.
    *
    * @param desc The descriptor for the object.
    * @param obj The ID of the object to compact.
    */
  def compact (desc: PageDescriptor [_], obj: ObjectId): Unit

  /** Release the pages held by the object's generations.
    *
    * @param desc The descriptor for the object.
    * @param obj The ID of the object.
    * @param gens The generations to release.
    */
  def release (desc: PageDescriptor [_], obj: ObjectId, gens: Set [Long]): Unit

  /** Join a release epoch, and leave it when the async task completes. Pages which are live at the
    * beginning of the epoch will remain available, even if they scheduled for release during the
    * epoch.
    *
    * The task may have disk pointers to pages ([[Position]]), for example in a local variable of
    * a method or in a member field of an object. It may also indirectly have pointers, for example
    * through a tree of index nodes. All these pages will remain available until the task completes.
    */
  def join [A] (task: Async [A]): Async [A]
}

object Disk {

  /** This has been moved to package level for easier access in the Scaladoc. */
  @deprecated ("Use DiskConfig", "0.3.0")
  type Config = DiskConfig

  /** This has been moved to package level for easier access in the Scaladoc. */
  @deprecated ("Use DiskConfig", "0.3.0")
  val Config = DiskConfig

  /** This has been moved to package level for easier access in the Scaladoc. */
  @deprecated ("Use DiskController", "0.3.0")
  type Controller = DiskController

  /** This has been moved to package level for easier access in the Scaladoc. */
  @deprecated ("Use DiskLaunch", "0.3.0")
  type Launch = DiskLaunch

  /** This has been moved to package level for easier access in the Scaladoc. */
  @deprecated ("Use DiskRecovery", "0.3.0")
  type Recovery = DiskRecovery

  /** Initialize the disk system.
    *
    * Setup the disk system with one or more disks; all disks get the same geometry. This does
    * not yield a [[Disk]] system to use immediately. It only writes superblocks and metadata
    * for disk recovery. After initializing the disks, call [[Disk#recover Disk.recover]].
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
      sysid: SystemId,
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
  def recover () (implicit scheduler: Scheduler, config: DiskConfig): DiskRecovery =
    new RecoveryAgent
}
