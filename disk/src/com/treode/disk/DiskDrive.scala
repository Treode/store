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
import java.util.{Arrays, ArrayDeque}
import java.util.concurrent.Executors
import scala.collection.JavaConversions._
import scala.collection.mutable.UnrolledBuffer
import scala.util.{Failure, Success, Try}

import com.treode.async.{Async, Callback, Fiber, Latch, Scheduler}
import com.treode.async.implicits._
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

import Async.{async, guard, latch, when}
import Callback.{fanout, ignore}
import DiskDrive.offset
import RecordHeader._

private class DiskDrive (
    val id: Int,
    val path: Path,
    val file: File,
    val geom: DriveGeometry,
    val alloc: Allocator,
    val kit: DiskKit,
    val logBuf: PagedBuffer,
    var draining: Boolean,
    var logSegs: ArrayDeque [Int],
    var logHead: Long,
    var logTail: Long,
    var logLimit: Long,
    var pageSeg: SegmentBounds,
    var pageHead: Long,
    var pageLedger: PageLedger,
    var pageLedgerDirty: Boolean
) {
  import geom.{blockAlignDown, blockAlignUp, blockBytes, segmentBounds, segmentCount}
  import kit.{checkpointer, compactor, config, drives, scheduler}

  val fiber = new Fiber
  val logmp = new Multiplexer [PickledRecord] (kit.logd)
  val logr: (Long, UnrolledBuffer [PickledRecord]) => Unit = (receiveRecords _)
  val pagemp = new Multiplexer [PickledPage] (kit.paged)
  val pager: (Long, UnrolledBuffer [PickledPage]) => Unit = (receivePages _)
  var compacting = IntSet()
  var open = true

  def record (entry: RecordHeader): Async [Unit] = {
    async { cb =>
      logmp.send (PickledRecord (id, entry, cb))
    }}

  def digest: Async [DriveDigest] =
    fiber.supply {
      val allocated = segmentCount - alloc.free.size
      new DriveDigest (path, geom, allocated, draining)
    }

  def added() {
    if (draining) {
      logmp.pause() run (ignore)
      pagemp.pause() run (ignore)
    }
    logmp.receive (logr)
    pagemp.receive (pager)
  }

  def mark(): Async [(Int, Long)] =
    fiber.supply {
      (id, logTail)
    }

  private def _checkpoint1 (mark: Long): Async [Unit] =
    fiber.supply {
      logHead = mark
      val release = Seq.newBuilder [Int]
      while (! (segmentBounds (logSegs.peek) .contains (logHead)))
        release += logSegs.remove()
      val nums = IntSet (release.result.sorted: _*)
      alloc.free (nums)
    }

  private def _checkpoint2(): Async [Unit] =
    fiber.guard {
      record (Checkpoint (pageHead, pageLedger.zip))
    }

  private def _checkpoint3 (boot: BootBlock): Async [Unit] =
    fiber.guard {
      val superb = SuperBlock (id, boot, geom, draining, alloc.free, logHead)
      SuperBlock.write (superb, file)
    }

  private def _checkpoint (boot: BootBlock, mark: Long): Async [Unit] = {
    for {
      _ <- _checkpoint1 (mark)
      _ <- pagemp.pause()
      _ <- _checkpoint2()
      _ <- _checkpoint3 (boot)
    } yield {
      pagemp.resume()
    }}

  def checkpoint (boot: BootBlock, mark: Option [Long]): Async [Unit] =
    mark match {
      case Some (mark) => _checkpoint (boot, mark)
      case None => _checkpoint3 (boot)
    }

  private def _protected: IntSet = {
    if (draining)
      IntSet (logSegs.toSeq.sorted: _*)
    else
      IntSet ((pageSeg.num +: logSegs.toSeq).sorted: _*)
  }

  private def _cleanable: Iterable [SegmentPointer] = {
    val skip = _protected.add (compacting)
    for (seg <- alloc .cleanable (skip))
      yield SegmentPointer (this, segmentBounds (seg))
  }

  def cleanable(): Async [Iterable [SegmentPointer]] =
    fiber.supply {
      _cleanable
    }

  def compacting (segs: Seq [SegmentPointer]): Unit =
    fiber.execute {
      val nums = IntSet (segs.map (_.num) .sorted: _*)
      compacting = compacting.add (nums)
    }

  def free (seg: SegmentPointer): Unit =
    fiber.execute {
      if (open) {
        val nums = IntSet (seg.num)
        assert (!(_protected contains seg.num))
        compacting = compacting.remove (nums)
        alloc.free (nums)
        record (SegmentFree (nums)) run (ignore)
        if (draining && alloc.drained (_protected))
          drives.detach (this)
      }}

  private def _writeLedger(): Async [Unit] =
    when (pageLedgerDirty) {
      pageLedgerDirty = false
      PageLedger.write (pageLedger.clone(), file, geom, pageSeg.base, pageHead)
    }

  def drain(): Async [Iterable [SegmentPointer]] =
    fiber.guard {
      for {
        _ <- latch (logmp.pause(), pagemp.close())
        _ <- _writeLedger()
        _ <- record (DiskDrain (pageSeg.num))
        _ = draining = true
        segs <- cleanable()
      } yield {
        segs
      }}

  def detach(): Async [Unit] =
    fiber.guard {
      open = false
      logmp.close()
      .ensure (file.close())
    }

  def close(): Async [Unit] =
    fiber.guard {
      open = false
      for {
        _ <- pagemp.close()
        _ <- logmp.close()
      } yield {
        file.close()
      }}

  private def splitRecords (entries: UnrolledBuffer [PickledRecord]) = {
    val accepts = new UnrolledBuffer [PickledRecord]
    val rejects = new UnrolledBuffer [PickledRecord]
    var pos = logTail
    var realloc = false
    for (entry <- entries) {
      if (entry.disk.isDefined && entry.disk.get != id) {
        rejects.add (entry)
      } else if (draining && entry.disk.isEmpty) {
        rejects.add (entry)
      } else if (pos + entry.byteSize + RecordHeader.trailer < logLimit) {
        accepts.add (entry)
        pos += entry.byteSize
      } else {
        rejects.add (entry)
        realloc = true
      }}
    (accepts, rejects, realloc)
  }

  private def writeRecords (buf: PagedBuffer, batch: Long, entries: Seq [PickledRecord]) = {
    val start = buf.writePos
    val callbacks = new UnrolledBuffer [Callback [Unit]]
    for (entry <- entries) {
      entry.write (batch, buf)
      callbacks.add (entry.cb)
    }
    val end = buf.writePos
    (callbacks, end - start)
  }

  private def reallocRecords(): Async [Unit] = {

    val newBuf = PagedBuffer (geom.blockBits)
    val newSeg = alloc.alloc (geom, config)
    logSegs.add (newSeg.num)
    RecordHeader.pickler.frame (LogEnd, newBuf)
    newBuf.writePos = blockAlignUp (newBuf.writePos)

    val wpos = blockAlignDown (logTail)
    RecordHeader.pickler.frame (LogAlloc (newSeg.num), logBuf)
    logBuf.writePos = blockAlignUp (logBuf.writePos)
    assert (wpos + logBuf.readableBytes <= logLimit, s"$logBuf $wpos $logLimit")

    for {
      _ <- file.flush (newBuf, newSeg.base)
      _ <- file.flush (logBuf, wpos)
    } yield fiber.execute {
      logBuf.clear()
      logTail = newSeg.base
      logLimit = newSeg.limit
      logmp.receive (logr)
    }}

  private def advanceRecords(): Async [Unit] = {

    val mark = logBuf.writePos
    val len = logBuf.readableBytes
    val wpos = blockAlignDown (logTail)
    RecordHeader.pickler.frame (LogEnd, logBuf)
    logBuf.writePos = blockAlignUp (logBuf.writePos)
    assert (wpos + logBuf.readableBytes <= logLimit, s"$logBuf $wpos $logLimit")

    for {
      _ <- file.flush (logBuf, wpos)
    } yield fiber.execute {
      val rpos = blockAlignDown (mark)
      logBuf.readPos = rpos
      logBuf.writePos = mark
      logBuf.discard (rpos)
      logTail = wpos + len
      logmp.receive (logr)
    }}

  def receiveRecords (batch: Long, entries: UnrolledBuffer [PickledRecord]): Unit =
    fiber.execute {

      val (accepts, rejects, realloc) = splitRecords (entries)
      logmp.send (rejects)
      val (callbacks, length) = writeRecords (logBuf, batch, accepts)
      val cb = fanout (callbacks)
      checkpointer.tally (length, accepts.size)

      if (realloc)
        reallocRecords() run cb
      else
        advanceRecords() run cb
    }

  private def splitPages (pages: UnrolledBuffer [PickledPage]) = {
    val projector = pageLedger.project
    val limit = (pageHead - pageSeg.base).toInt
    val accepts = new UnrolledBuffer [PickledPage]
    val rejects = new UnrolledBuffer [PickledPage]
    var totalBytes = 0
    var realloc = false
    for (page <- pages) {
      projector.add (page.typ, page.obj, page.group)
      val pageBytes = blockAlignUp (page.byteSize)
      val ledgerBytes = blockAlignUp (projector.byteSize)
      if (totalBytes + ledgerBytes + pageBytes < limit) {
        accepts.add (page)
        totalBytes += pageBytes
      } else {
        rejects.add (page)
        realloc = true
      }}
    (accepts, rejects, realloc)
  }

  private def writePages (pages: UnrolledBuffer [PickledPage]) = {
    val buffer = PagedBuffer (geom.blockBits)
    val callbacks = new UnrolledBuffer [Callback [Long]]
    val ledger = new PageLedger
    for (page <- pages) {
      val start = buffer.writePos
      page.write (buffer)
      buffer.writePos = blockAlignUp (buffer.writePos)
      val length = buffer.writePos - start
      callbacks.add (offset (id, start, length, page.cb))
      ledger.add (page.typ, page.obj, page.group, length)
    }
    (buffer, callbacks, ledger)
  }

  private def reallocPages (ledger: PageLedger): Async [Unit] = {
    compactor.tally (1)
    pageLedger.add (ledger)
    pageLedgerDirty = true
    for {
      _ <- PageLedger.write (pageLedger, file, geom, pageSeg.base, pageHead)
      _ <- record (PageClose (pageSeg.num))
    } yield fiber.execute {
      pageSeg = alloc.alloc (geom, config)
      pageHead = pageSeg.limit
      pageLedger = new PageLedger
      pageLedgerDirty = true
      pagemp.receive (pager)
    }}

  private def advancePages (pos: Long, ledger: PageLedger): Async [Unit] = {
    for {
      _ <- record (PageWrite (pos, ledger.zip))
    } yield fiber.execute {
      pageHead = pos
      pageLedger.add (ledger)
      pageLedgerDirty = true
      pagemp.receive (pager)
    }}

  def receivePages (batch: Long, pages: UnrolledBuffer [PickledPage]): Unit =
    fiber.execute {

      val (accepts, rejects, realloc) = splitPages (pages)
      pagemp.send (rejects)
      val (buffer, callbacks, ledger) = writePages (accepts)
      val pos = pageHead - buffer.readableBytes
      assert (pos >= pageSeg.base + blockBytes)
      val cb = fanout (callbacks)

      val task = for {
        _ <- file.flush (buffer, pos)
        _ <- if (realloc)
              reallocPages (ledger)
            else
              advancePages (pos, ledger)
      } yield pos
      task run cb
    }

  override def toString = s"DiskDrive($path, $geom)"
}

private object DiskDrive {

  def offset (id: Int, offset: Long, length: Int, cb: Callback [Position]): Callback [Long] =
    cb.continue (base => Some (Position (id, base + offset, length)))

  def read [P] (file: File, geom: DriveGeometry, desc: PageDescriptor [_, P], pos: Position): Async [P] =
    guard {
      val buf = PagedBuffer (geom.blockBits)
      for (_ <- file.fill (buf, pos.offset, pos.length))
        yield desc.ppag.unpickle (buf)
    }

  def init (
      id: Int,
      path: Path,
      file: File,
      geom: DriveGeometry,
      boot: BootBlock,
      kit: DiskKit
  ): Async [DiskDrive] =

    guard {
      import kit.config

      val alloc = Allocator (geom, config)
      val logBuf = PagedBuffer (geom.blockBits)
      val logSeg = alloc.alloc (geom, config)
      val logSegs = new ArrayDeque [Int]
      logSegs.add (logSeg.num)

      val superb = SuperBlock (id, boot, geom, false, alloc.free, logSeg.base)

      for {
        _ <- latch (
            SuperBlock.write (superb, file),
            SuperBlock.clear (boot.gen + 1, file),
            RecordHeader.init (file, geom, logSeg.base))
      } yield {
        val pageSeg = alloc.alloc (geom, config)
        new DiskDrive (id, path, file, geom, alloc, kit, logBuf, false, logSegs, logSeg.base,
            logSeg.base, logSeg.limit, pageSeg, pageSeg.limit, new PageLedger, true)
       }}

  def init (
      id: Int,
      path: Path,
      file: File,
      geom: DriveGeometry,
      boot: BootBlock
  ) (implicit
      config: Disk.Config
  ): Async [Unit] =
    guard {

      val alloc = Allocator (geom, config)
      val logSeg = alloc.alloc (geom, config)
      val logSegs = new ArrayDeque [Int]
      logSegs.add (logSeg.num)

      val superb = SuperBlock (id, boot, geom, false, alloc.free, logSeg.base)

      for {
        _ <- latch (
            SuperBlock.write (superb, file),
            SuperBlock.clear (boot.gen + 1, file),
            RecordHeader.init (file, geom, logSeg.base))
      } yield ()
    }

  def init (
      sysid: Array [Byte],
      items: Seq [(Path, File, DriveGeometry)]
  ) (implicit
      scheduler: Scheduler,
      config: Disk.Config
  ): Async [Unit] =
    guard {
      val attaching = items.setBy (_._1)
      require (!items.isEmpty, "Must list at least one file or device to initialize.")
      require (attaching.size == items.size, "Cannot initialize a path multiple times.")
      val boot = BootBlock.apply (sysid, 0, items.size, attaching)
      for {
        _ <-
        for (((path, file, geom), id) <- items.zipWithIndex.latch)
          init (id, path, file, geom, boot)
      } yield {
        log.initializedDrives (attaching)
      }}

  def init (
      sysid: Array [Byte],
      superBlockBits: Int,
      segmentBits: Int,
      blockBits: Int,
      diskBytes: Long,
      paths: Seq [Path]
  ) {
    val executor = Executors.newSingleThreadScheduledExecutor()
    var items = Seq.empty [Try [(Path, File, DriveGeometry)]]
    try {
      implicit val scheduler = Scheduler (executor)
      implicit val config = Disk.Config.suggested.copy (superBlockBits = superBlockBits)
      val geom = DriveGeometry (segmentBits, blockBits, diskBytes)
      items =
        for (path <- paths)
          yield Try ((path, openFile (path, geom), geom))
      init (sysid, items.map (_.get)) .await()
    } finally {
      items foreach {
        case Success ((_, file, _)) => file.close()
        case Failure (_) => ()
      }
      executor.shutdown()
    }}}
