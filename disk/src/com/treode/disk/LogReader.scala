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
import java.util.ArrayDeque
import scala.collection.mutable.UnrolledBuffer
import scala.util.{Failure, Success}

import com.treode.async.{Async, BatchIterator, Callback, Scheduler}, Async.{async, guard, supply}
import com.treode.async.implicits._
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{InvalidTagException, Picklers}

import LogControl._

private class LogReader (
  path: Path,
  file: File,
  superb: SuperBlock,
  records: RecordRegistry,
  collector: LogCollector
) (implicit
  scheduler: Scheduler,
  config: DiskConfig
) extends BatchIterator [LogEntries] {

  import superb.{draining, id, geom, logHead}
  import superb.geom.{blockAlignDown, blockBits, blockBytes}

  class Batch (f: Iterable [LogEntries] => Async [Unit], var cb: Callback [Unit]) {

    /** The allocator for the drive; needed now to track reallocation of log segments. */
    private val alloc = new SegmentAllocator (geom)

    /** The buffer for this log; passed to the final LogWriter so that it can start appending
      * just after the point where this reader finishes replaying.
      */
    private val buf = PagedBuffer (blockBits)

    /** The segments allocated to this log; passed to the final LogWriter so that it knows which
      * segments to release after completing a checkpoint.
      */
    private val segs = new ArrayDeque [Int]

    /** The segment that we are currently reading. */
    private var seg = SegmentBounds (geom.segmentNum (logHead), geom, config)

    /** The position at which we currently know the log has been flushed. */
    private var flushed = logHead

    /** The position at which we are currenlty reading. */
    private var pos = logHead

    /** The number of the last batch replayed by this reader; fed to the collector so that it can
      * compute the greatest batch number replayed by all reader.
      */
    private var batch = 0L

    /** The total count of bytes replayed by this reader; fed to the collector so that it can
      * compute the total bytes replayed by all readers.
      */
    private var bytes = 0L

    /** The total count of entries replayed by this reader; fed to the collector so that it can
      * compute the total entries replayed by all readers.
      */
    private var entries = 0L

    /** We have already recorded the allocation of this segment. Now start reading it. */
    private def readSegmentTag(): Async [Option [LogEntries]] =
      for {
        _ <- file.fill (buf, pos, SegmentTagSpace, blockBits)
      } yield {
        flushed = pos
        val tag = buf.readLong()
        if (tag == SegmentTag) {
          // The segment is off to a good start. Continue reading.
          pos += SegmentTagSpace
          Some (LogEntries.empty)
        } else {
          // We just happen to crash after writing the last block of the previous segment, which
          // included the AllocTag, but before writing the first block of this segment. In other
          // words, we've reached the end of the log, and we start appending at this point.
          buf.clear()
          buf.writeLong (SegmentTag)
          None
        }}

    /** We need to record the allocation of this segment, then start reading it. */
    private def readSegmentTag (num: Int): Async [Option [LogEntries]] = {
      seg = alloc.alloc (num)
      pos = seg.base
      segs.add (seg.num)
      buf.clear()
      readSegmentTag()
    }

    private def readRecords (length: Int, count: Int): Seq [Unit => Any] =
      try {
        val rs = records.read (buf, length, count)
        buf.discard (buf.readPos)
        rs
      } catch {
        case t: InvalidTagException => throw t
        case t: Throwable => throw new Exception (s"Error reading records from $path", t)
      }

    private def readLogControl (tag: Long): Async [Option [LogEntries]] =
      tag match {

        case EntriesTag =>
          for {
            _ <- file.fill (buf, pos + LogTagSize, LogEntriesSpace - LogTagSize, blockBits)
            batch = buf.readLong()
            length = buf.readInt()
            count = buf.readInt()
            _ <- file.fill (buf, pos + LogEntriesSpace, length, blockBits)
          } yield {
            if (this.batch < batch) {
              // This is a valid batch. We must replay the entries.
              this.batch = batch
              bytes += length
              entries += count
              flushed = pos
              pos += length + LogEntriesSpace
              Some (LogEntries (batch, readRecords (length, count)))
            } else {
              // This looks like a batch, but it slips back in time. We actually reached the end
              // of the log. It just happened to occur immediately before a batch that was long
              // ago checkpointed and tossed.
              val end = buf.readPos - LogEntriesSpace
              buf.readPos = blockAlignDown (end)
              buf.writePos = end
              flushed = pos
              pos -= end
              None
            }}

        case AllocTag =>
          for {
            _ <- file.fill (buf, pos + LogTagSize, LogAllocSpace - LogTagSize, blockBits)
            more <- readSegmentTag (buf.readInt())
          } yield {
            more
          }

        case EndTag =>
          val end = buf.readPos - LogEndSpace
          buf.readPos = blockAlignDown (end)
          buf.writePos = end
          pos -= end
          supply (None)

        case _ =>
          // The log is corrupt.
          val tagPos = pos + buf.readPos - LogTagSize
          throw new AssertionError (f"The log is corrupt at $tagPos of $path")
      }

    /** The buffer has been filled upto a block boundary. Now expect a batch. */
    private def readBatch(): Async [Option [LogEntries]] =
      for {
        _ <- file.fill (buf, pos, LogTagSize, blockBits)
        batch <- readLogControl (buf.readLong())
      } yield {
        batch
      }

    /** The buffer is unfilled. Align the first read to a block boundary, then expect a batch. */
    private def readFirstBatch(): Async [Option [LogEntries]] = {
      val start = blockAlignDown (pos)
      val rpos = (pos - start).toInt
      for {
        _ <- file.fill (buf, start, rpos + LogTagSize, blockBits)
        _ = buf.readPos = rpos
        batch <- readLogControl (buf.readLong())
      } yield {
        batch
      }}

    private def _fail (t: Throwable) {
      val cb = this.cb
      this.cb = null
      cb.fail (t)
    }

    private def _close() {
      val drive =
        new BootstrapDrive (id, path, file, geom, draining, alloc, buf, segs, pos, seg.limit, logHead, flushed)
      collector.reattach (drive, batch, bytes, entries)
      val cb = this.cb
      this.cb = null
      cb.pass (())
    }

    private val _next: Callback [Unit] = {
      case Success (_) => next()
      case Failure (t) => _fail (t)
    }

    private val _give: Callback [Option [LogEntries]] = {
      case Success (Some (batch)) =>
        f (Iterable (batch)) run (_next)
      case Success (None) =>
        _close()
      case Failure (t) =>
        _fail (t)
    }

    def start(): Unit = {
      if (pos == 0) {
        _close()
      } else if (pos == seg.base) {
        alloc.alloc (seg.num)
        segs.add (seg.num)
        readSegmentTag() .run (_give)
      } else {
        alloc.alloc (seg.num)
        segs.add (seg.num)
        readFirstBatch() .run (_give)
      }
    }

    def next(): Unit =
      if (pos == 0)
        _close()
      else if (pos == seg.base)
        readSegmentTag() .run (_give)
      else
        readBatch() .run (_give)
  }

  def batch (f: Iterable [LogEntries] => Async [Unit]): Async [Unit] =
    async (new Batch (f, _) .start())
}
