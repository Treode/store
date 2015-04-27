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
import java.util.ArrayDeque
import scala.collection.mutable.UnrolledBuffer
import scala.util.{Failure, Success}

import com.treode.async.{Async, BatchIterator, Callback, Scheduler}, Async.{async, guard, supply}
import com.treode.async.implicits._
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.disk.{Disk, DiskConfig, DriveGeometry, IntSet, RecordDescriptor, RecordRegistry,
  SegmentBounds, TypeId}
import com.treode.pickle.{InvalidTagException, Picklers}

import LogControl._

private class LogReader (
  path: Path,
  file: File,
  superb: SuperBlock,
  records: RecordRegistry,
  replayer: LogReplayer,
  logdsp: LogDispatcher,
  pagdsp: PageDispatcher
) (implicit
  scheduler: Scheduler,
  config: DiskConfig
) extends BatchIterator [LogEntries] {

  import superb.{draining, id, geom, logHead}
  import superb.geom.{blockAlignDown, blockBits, blockBytes}

  class Batch (f: Iterable [LogEntries] => Async [Unit], var cb: Callback [Unit]) {

    private val alloc = new SegmentAllocator (geom)
    private val buf = PagedBuffer (blockBits)
    private val segs = new ArrayDeque [Int]
    private var seg = SegmentBounds (geom.segmentNum (logHead), geom, config)
    private var flushed = logHead
    private var pos = logHead
    private var batch = 0L

    private def readSegmentTag(): Async [Option [LogEntries]] =
      for {
        _ <- file.fill (buf, pos, SegmentTagSpace, blockBits)
      } yield {
        flushed = pos
        val tag = buf.readLong()
        if (tag == SegmentTag) {
          pos += SegmentTagSpace
          Some (LogEntries.empty)
        } else {
          buf.clear()
          buf.writeLong (SegmentTag)
          None
        }}

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
              this.batch = batch
              flushed = pos
              pos += length + LogEntriesSpace
              Some (LogEntries (batch, readRecords (length, count)))
            } else {
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
          val tagPos = pos + buf.readPos - LogTagSize
          throw new AssertionError (f"Unrecognized tag $tag%X at $tagPos of $path")
      }

    private def readBatch(): Async [Option [LogEntries]] =
      for {
        _ <- file.fill (buf, pos, LogTagSize, blockBits)
        batch <- readLogControl (buf.readLong())
      } yield {
        batch
      }

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

      val logdtch = new Detacher (superb.draining)
      val logwrtr =
        new LogWriter (path, file, geom, logdsp, logdtch, alloc, buf, segs, pos, seg.limit, logHead, flushed)

      val pagdtch = new Detacher (superb.draining)
      val pagwrtr = new PageWriter (id, path, file, geom, pagdsp, pagdtch, alloc)

      val drive = new Drive (file, geom, alloc, logwrtr, pagwrtr, draining, id, path)
      replayer.reattach (drive)

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
