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

package com.treode.disk.edit

import java.nio.file.Path
import java.util.{ArrayDeque, ArrayList}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConversions._
import scala.collection.mutable.UnrolledBuffer
import scala.util.{Failure, Success}

import com.treode.async.{Async, Callback, Scheduler}, Async.{async, guard, supply}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.disk.{DriveGeometry, SegmentBounds}

import LogControl._

private class LogWriter (
  path: Path,
  file: File,
  geom: DriveGeometry,
  dispatcher: LogDispatcher,
  detacher: Detacher,
  alloc: SegmentAllocator,

  // We pickle records into this buffer. We discard it page by page as we flush it to disk. The
  // first page retains the data from the previous flush so that the next flush can remain aligned
  // a block boundary.
  buf: PagedBuffer,

  private var segs: ArrayDeque [Int],

  // The current position in the current segment. This corresponds to position 0 in buffer, and we
  // increment it when we discard pages from the buffer.
  private var pos: Long,

  // The limit of the current segment, exclusive.
  private var limit: Long,

  // The position of the latest batch that was written when a checkpoint started, and that
  // checkpoint has completed. This is the last flushed batch before a completed checkpoint had
  // begun. The log replay of the next årecovery starts at this point.
  private var checkpointed: Long,

  // The position of the latest batch that was written.
  private var flushed: Long
) (implicit
  scheduler: Scheduler
) {

  import geom.{blockBits, blockAlignDown, blockAlignUp}

  // The position of the latest batch that was written when a checkpoint started. This is the last
  // flushed batch when a checkpoint began, but that checkpoint may not have completed.
  private var marked = flushed

  // Things awaiting draining to complete.
  private var drains = List.empty [Callback [Unit]]

  private def isDrained: Boolean =
    checkpointed == flushed

  private def run [A] (task: Async [A]) {
    task run {
      case Success (_) => ()
      case Failure (t) => throw t
    }}

  private def register(): Unit =
    run {
      for {
        (batch, records) <- dispatcher.receive()
        enroll <- flush (batch, records)
      } yield {
        if (enroll)
          register()
      }}

  private def flush (batch: Long, records: UnrolledBuffer [PickledRecord]): Async [Boolean] =
    guard {

      if (!detacher.startFlush()) {
        dispatcher.send (records)
        supply (false)
      } else {

        // Compute how many bytes we can write without overflowing the segment. We need to allow
        // enough for the marker for the next batch and the alloc marker after it.
        val bytes = limit - pos - LogEntriesSpace - LogAllocSpace

        // Remember where we started, and leave space for the marker between batches.
        val start = buf.writePos
        buf.writePos = start + LogEntriesSpace

        // Fit as many records as possible into the remainder of this segment; track which fit
        // (callbacks) and which to do not fit (putbacks).
        val callbacks = new ArrayList [Callback [Unit]] (records.size)
        val putbacks = new UnrolledBuffer [PickledRecord]
        for (item <- records) {
          if (buf.readableBytes + item.byteSize < bytes) {
            item.write (buf)
            callbacks.add (item.cb)
          } else {
            putbacks += item
          }}

        // Double check to avoid writing over the segment boundary.
        assert (buf.readableBytes + pos <= limit)

        // Remember where we parked.
        val end = buf.writePos

        val alloc: Option [SegmentBounds] =

          // If all the records did fit into the segment.
          if (putbacks.isEmpty) {

            // Record the end of this segment.
            buf.writeLong (EndTag)
            None

          // If some records did not fit into the segment.
          } else {

            // Return the mis-fits to be flushed latter.
            dispatcher.send (putbacks)

            // Allocate a new segment and record it in the log.
            val seg = this.alloc.alloc()
            buf.writeLong (AllocTag)
            buf.writeInt (seg.num)
            Some (seg)
          }

        // Return to the start to write the tag, byte count and record count.
        buf.writePos = start
        buf.writeLong (EntriesTag)
        buf.writeLong (batch)
        val length = end - start - LogEntriesSpace
        buf.writeInt (length)
        buf.writeInt (callbacks.size)

        // Advance the end to align on a block.
        if (alloc.isDefined)
          buf.writePos = blockAlignUp (end + LogAllocSpace)
        else
          buf.writePos = blockAlignUp (end + LogEndSpace)

        for {
          _ <- file.flush (buf, pos)
        } yield {

          // Setup for the next batch.
          alloc match {

            case Some (seg) =>

              // Start fresh for the new segment. Write the log segment marker.
              segs.add (seg.num)
              limit = seg.limit
              pos = seg.base
              buf.clear()
              buf.writeLong (SegmentTag)

              synchronized (flushed = pos)

            case None =>

              synchronized (flushed = pos + end)

              // Reset the buffer to use this segment for the next batch.
              val next = blockAlignDown (end)
              buf.readPos = next
              buf.writePos = end
              pos += buf.discard (next)
          }

          // Acknowledge the writes.
          for (cb <- callbacks)
            scheduler.pass (cb, ())

          // Can we enroll for another batch?
          detacher.finishFlush()
        }}}

  def launch(): Unit =
    register()

  def startCheckpoint(): Unit =
    synchronized (marked = flushed)

  def getCheckpoint (finish: Boolean): Long =
    synchronized {
      if (finish) {
        checkpointed = marked
        val head = geom.segmentNum (checkpointed)
        while (segs.peek != head && segs.size > 1)
          alloc.free (segs.remove())
        if (!drains.isEmpty && isDrained) {
          if (checkpointed != 0L) {
            while (!segs.isEmpty)
              alloc.free (segs.remove())
            flushed = 0L
            marked = flushed
            checkpointed = flushed
          }
          for (cb <- drains)  scheduler.pass (cb, ())
          drains = List.empty
        }}
      checkpointed
    }

  def startDraining(): Async [Unit] =
    detacher.startDraining()

  def awaitDrained(): Async [Unit] =
    async { cb =>
      synchronized {
        if (isDrained) {
          if (checkpointed != 0L) {
            while (!segs.isEmpty)
              alloc.free (segs.remove())
            flushed = 0L
            marked = flushed
            checkpointed = flushed
          }
          scheduler.pass (cb, ())
        } else {
          drains ::= cb
        }}}}
