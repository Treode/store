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
import scala.util.{Failure, Success}

import com.treode.async.{Async, BatchIterator, Callback, Scheduler}
import com.treode.async.implicits._
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

import Async.{async, guard, supply}
import RecordHeader._
import SuperBlocks.chooseSuperBlock

private class LogIterator private (
    records: RecordRegistry,
    path: Path,
    file: File,
    logBuf: PagedBuffer,
    superb: SuperBlock,
    alloc: Allocator,
    logSegs: ArrayDeque [Int],
    private var logSeg: SegmentBounds
) (implicit
    scheduler: Scheduler,
    config: Disk.Config
) extends BatchIterator [(Long, Unit => Any)] {

  import superb.{geometry => geom, id}
  import superb.geometry.{blockAlignDown, blockBits, segmentNum}

  private var draining = superb.draining
  private var logPos = superb.logHead
  private var pagePos = Option.empty [Long]
  private var pageLedger = new PageLedger

  class Batch (f: Iterator [(Long, Unit => Any)] => Async [Unit], cb: Callback [Unit]) {

    val _read: Callback [Int] = {
      case Success (v) => cb.defer (read (v))
      case Failure (t) => fail (t)
    }

    val _next: Callback [Unit] = {
      case Success (v) => cb.defer (next())
      case Failure (t) => fail (t)
    }

    def fail (t: Throwable) {
      logPos = -1L
      scheduler.fail (cb, t)
    }

    def read (len: Int) {
      val start = logBuf.readPos
      val hdr = RecordHeader.pickler.unpickle (logBuf)
      hdr match {

        case LogEnd =>
          logBuf.readPos = start - 4
          scheduler.pass (cb, ())

        case LogAlloc (next) =>
          logSeg = alloc.alloc (next, geom, config)
          logSegs.add (logSeg.num)
          logPos = logSeg.base
          logBuf.clear()
          file.deframe (logBuf, logPos, blockBits) run (_read)

        case PageWrite (pos, _ledger) =>
          val num = segmentNum (pos)
          alloc.alloc (num, geom, config)
          pagePos = Some (pos)
          pageLedger.add (_ledger)
          logPos += len + 4
          file.deframe (logBuf, logPos, blockBits) run (_read)

        case PageClose (num) =>
          alloc.alloc (num, superb.geometry, config)
          pagePos = None
          pageLedger = new PageLedger
          logPos += len + 4
          file.deframe (logBuf, logPos, blockBits) run (_read)

        case SegmentFree (nums) =>
          alloc.free (nums)
          logPos += len + 4
          file.deframe (logBuf, logPos, blockBits) run (_read)

        case Checkpoint (pos, _ledger) =>
          val num = segmentNum (pos - 1)
          alloc.alloc (num, superb.geometry, config)
          pagePos = Some (pos)
          pageLedger = _ledger.unzip
          logPos += len + 4
          file.deframe (logBuf, logPos, blockBits) run (_read)

        case DiskDrain (num) =>
          draining = true
          pagePos = None
          pageLedger = new PageLedger
          logPos += len + 4
          file.deframe (logBuf, logPos, blockBits) run (_read)

        case Entry (batch, id) =>
          val end = logBuf.readPos
          val entry = records.read (id.id, logBuf, len - end + start)
          logPos += len + 4
          f (Iterator ((batch, entry))) run (_next)
      }}

    def next() {
      file.deframe (logBuf, logPos, blockBits) run (_read)
    }}

  def batch (f: Iterator [(Long, Unit => Any)] => Async [Unit]): Async [Unit] =
    async (new Batch (f, _) .next())

  def pages(): (SegmentBounds, Long, PageLedger, Boolean) =
    pagePos match {
      case _ if draining =>
        val seg = SegmentBounds (-1, -1L, -1L)
        (seg, -1L, new PageLedger, false)
      case Some (pos) =>
        val num = segmentNum (pos - 1)
        val seg = alloc.alloc (num, geom, config)
        (seg, pos, pageLedger, true)
      case None =>
        val seg = alloc.alloc (geom, config)
        (seg, seg.limit, pageLedger, true)
    }

  def close (kit: DiskKit): DiskDrive = {
    val (seg, head, ledger, dirty) = pages()
    val rpos = blockAlignDown (logBuf.readPos)
    logBuf.writePos = logBuf.readPos
    logBuf.readPos = rpos
    logBuf.discard (rpos)
    new DiskDrive (superb.id, path, file, geom, alloc, kit, logBuf, draining, logSegs,
        superb.logHead, logPos, logSeg.limit, seg, head, ledger, dirty)
  }}

private object LogIterator {

  def apply (
      useGen0: Boolean,
      read: SuperBlocks,
      records: RecordRegistry
  ) (implicit
      scheduler: Scheduler,
      config: Disk.Config
  ): Async [(Int, LogIterator)] = {

    val path = read.path
    val file = read.file
    val superb = read.superb (useGen0)
    val geom = superb.geometry
    val alloc = Allocator (superb.free)
    val num = geom.segmentNum (superb.logHead)
    val logSeg = alloc.alloc (num, geom, config)
    val logSegs = new ArrayDeque [Int]
    logSegs.add (logSeg.num)
    val buf = PagedBuffer (geom.blockBits)

    val logBase = geom.blockAlignDown (superb.logHead)
    for {
      _ <- file.fill (buf, logBase, geom.blockBytes)
    } yield {
      buf.readPos = (superb.logHead - logBase).toInt
      val iter =
          new LogIterator (records, path, file, buf, superb, alloc, logSegs, logSeg)
      (superb.id, iter)
    }}

  def replay (
      reads: Seq [SuperBlocks],
      records: RecordRegistry
  ) (implicit
      scheduler: Scheduler,
      config: Disk.Config
  ): Async [DiskKit] = {

    val ordering = Ordering.by [(Long, Unit => Any), Long] (_._1)
    val useGen0 = chooseSuperBlock (reads)
    val boot = reads.head.superb (useGen0) .boot
    var logBatch = 0L

    def replay (_entry: (Long, Unit => Any)) {
      val (batch, entry) = _entry
      logBatch = batch
      entry (())
    }

    for {
      logs <- reads.latch.map foreach (apply (useGen0, _, records))
      iter = BatchIterator.merge (logs.values.toSeq) (ordering, scheduler)
      _ <- iter.foreach (replay _)
      kit = new DiskKit (boot.sysid, logBatch)
      drives =
        for (read <- reads) yield {
          val superb = read.superb (useGen0)
          logs (superb.id) .close (kit)
        }
      _ <- kit.drives.add (drives)
    } yield kit
  }}
