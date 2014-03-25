package com.treode.disk

import java.nio.file.Path
import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, UnrolledBuffer}
import scala.util.{Failure, Success}

import com.treode.async.{Async, AsyncConversions, Callback, Fiber, Latch}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

import Async.{async, guard, latch, when}
import AsyncConversions._
import Callback.{fanout, ignore}
import DiskDrive.offset
import RecordHeader._

private class DiskDrive (
    val id: Int,
    val path: Path,
    val file: File,
    val geometry: DiskGeometry,
    val alloc: Allocator,
    val kit: DisksKit,
    var draining: Boolean,
    var logSegs: ArrayBuffer [Int],
    var logHead: Long,
    var logTail: Long,
    var logLimit: Long,
    var logBuf: PagedBuffer,
    var pageSeg: SegmentBounds,
    var pageHead: Long,
    var pageLedger: PageLedger,
    var pageLedgerDirty: Boolean
) {
  import kit.{checkpointer, compactor, config, disks, scheduler}

  val fiber = new Fiber (scheduler)
  val logmp = new Multiplexer [PickledRecord] (kit.logd)
  val logr: UnrolledBuffer [PickledRecord] => Unit = (receiveRecords _)
  val pagemp = new Multiplexer [PickledPage] (kit.paged)
  val pager: UnrolledBuffer [PickledPage] => Unit = (receivePages _)

  def record (entry: RecordHeader): Async [Unit] =
    async (cb => logmp.send (PickledRecord (id, entry, cb)))

  def added() {
    logmp.receive (logr)
    pagemp.receive (pager)
  }

  def mark(): Async [Unit] =
    fiber.supply {
      logHead = logTail
    }

  private def writeLedger(): Async [Unit] = {
    when (pageLedgerDirty) {
      pageLedgerDirty = false
      PageLedger.write (pageLedger.clone(), file, pageSeg.pos)
    }}

  def checkpoint (boot: BootBlock): Async [Unit] =
    fiber.guard {
      val superb = SuperBlock (
          id, boot, geometry, draining, alloc.free, logSegs.head, logHead, pageSeg.num, pageHead)
      for {
        _ <- writeLedger()
        _ <- SuperBlock.write (superb, file)
      } yield ()
    }

  private def _protected: IntSet = {
    val protect = new ArrayBuffer [Int] (logSegs.size + 1)
    protect ++= logSegs
    if (!draining)
      protect += pageSeg.num
    IntSet (protect.sorted: _*)
  }

  private def _cleanable: Iterator [SegmentPointer] = {
    for (seg <- alloc.cleanable (_protected))
      yield SegmentPointer (this, geometry.segmentBounds (seg))
  }

  def cleanable(): Async [Iterator [SegmentPointer]] =
    fiber.supply {
      _cleanable
    }

  def free (segs: Seq [SegmentPointer]): Unit =
    fiber.execute {
      val nums = IntSet (segs.map (_.num) .sorted: _*)
      assert (!(nums intersects _protected))
      alloc.free (nums)
      record (SegmentFree (nums)) run (ignore)
      if (draining && alloc.drained (logSegs))
        disks.detach (this)
    }

  def drain(): Async [Iterator [SegmentPointer]] =
    fiber.guard {
      draining = true
      for {
        _ <- pagemp.close()
        _ <- latch (writeLedger(), record (DiskDrain))
        segs <- fiber.supply (_cleanable)
      } yield segs
    }

  def detach(): Unit =
    fiber.guard {
      for (_ <- logmp.close())
        yield file.close()
    } run (ignore)

  private def splitRecords (entries: UnrolledBuffer [PickledRecord]) = {
    // TODO: reject records that are too large
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

  private def writeRecords (buf: PagedBuffer, entries: Seq [PickledRecord]) = {
    val callbacks = new UnrolledBuffer [Callback [Unit]]
    for (entry <- entries) {
      entry.write (buf)
      callbacks.add (entry.cb)
    }
    callbacks
  }

  private def reallocRecords(): Async [Unit] = {
    val newBuf = PagedBuffer (12)
    val newSeg = alloc.alloc (geometry, config)
    RecordHeader.pickler.frame (LogEnd, newBuf)
    RecordHeader.pickler.frame (LogAlloc (newSeg.num), logBuf)
    for {
      _ <- file.flush (newBuf, newSeg.pos)
      _ <- file.flush (logBuf, logTail)
      _ <- fiber.supply {
          logSegs.add (newSeg.num)
          logTail = newSeg.pos
          logLimit = newSeg.limit
          logBuf.clear()
          logmp.receive (logr)
      }
    } yield ()
  }

  private def advanceRecords(): Async [Unit] = {
    val len = logBuf.readableBytes
    RecordHeader.pickler.frame (LogEnd, logBuf)
    for {
      _ <- file.flush (logBuf, logTail)
      _ <- fiber.supply {
          logTail += len
          logBuf.clear()
          logmp.receive (logr)
      }
    } yield ()
  }

  def receiveRecords (entries: UnrolledBuffer [PickledRecord]): Unit =
    fiber.execute {

      val (accepts, rejects, realloc) = splitRecords (entries)
      logmp.replace (rejects)

      val callbacks = writeRecords (logBuf, accepts)
      val cb = fanout (callbacks, scheduler)
      assert (logTail + logBuf.readableBytes <= logLimit)

      checkpointer.tally (logBuf.readableBytes, accepts.size)

      if (realloc)
        reallocRecords() run cb
      else
        advanceRecords() run cb
    }

  private def splitPages (pages: UnrolledBuffer [PickledPage]) = {
    // TODO: reject pages that are too large
    val projector = pageLedger.project
    val limit = (pageHead - pageSeg.pos).toInt
    val accepts = new UnrolledBuffer [PickledPage]
    val rejects = new UnrolledBuffer [PickledPage]
    var totalBytes = 0
    var realloc = false
    for (page <- pages) {
      projector.add (page.typ, page.obj, page.group)
      val pageBytes = geometry.blockAlignLength (page.byteSize)
      val ledgerBytes = geometry.blockAlignLength (projector.byteSize)
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
    val buffer = PagedBuffer (12)
    val callbacks = new UnrolledBuffer [Callback [Long]]
    val ledger = new PageLedger
    for (page <- pages) {
      val start = buffer.writePos
      page.write (buffer)
      buffer.writeZeroToAlign (geometry.blockBits)
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
      _ <- PageLedger.write (pageLedger, file, pageSeg.pos)
      _ <- fiber.guard {
          pageSeg = alloc.alloc (geometry, config)
          pageHead = pageSeg.limit
          pageLedger = new PageLedger
          pageLedgerDirty = true
          PageLedger.write (pageLedger, file, pageSeg.pos)
      }
      _ <- fiber.guard {
          pageLedgerDirty = false
          record (PageAlloc (pageSeg.num, ledger.zip))
      }
    } yield pagemp.receive (pager)
  }

  private def advancePages (pos: Long, ledger: PageLedger): Async [Unit] = {
    for {
      _ <- record (PageWrite (pos, ledger.zip))
      _ <- fiber.supply {
          pageHead = pos
          pageLedger.add (ledger)
          pageLedgerDirty = true
          pagemp.receive (pager)
      }
    } yield ()
  }

  def receivePages (pages: UnrolledBuffer [PickledPage]): Unit =
    fiber.execute {

      val (accepts, rejects, realloc) = splitPages (pages)
      pagemp.replace (rejects)

      val (buffer, callbacks, ledger) = writePages (accepts)
      val pos = pageHead - buffer.readableBytes
      assert (pos >= pageSeg.pos + geometry.blockBytes)
      val cb = fanout (callbacks, scheduler)

      val task = for {
        _ <- file.flush (buffer, pos)
        _ <- if (realloc)
              reallocPages (ledger)
            else
              advancePages (pos, ledger)
      } yield pos
      task run cb
    }}

private object DiskDrive {

  def offset (id: Int, offset: Long, length: Int, cb: Callback [Position]): Callback [Long] =
    cb.callback (base => Position (id, base + offset, length))

  def read [P] (file: File, desc: PageDescriptor [_, P], pos: Position): Async [P] =
    guard {
      val buf = PagedBuffer (12)
      for (_ <- file.fill (buf, pos.offset, pos.length))
        yield desc.ppag.unpickle (buf)
    }

  def init (
      id: Int,
      path: Path,
      file: File,
      geometry: DiskGeometry,
      boot: BootBlock,
      kit: DisksKit
  ): Async [DiskDrive] =

    guard {
      import kit.config

      val alloc = Allocator (geometry, config)
      val logSeg = alloc.alloc (geometry, config)
      val pageSeg = alloc.alloc (geometry, config)
      val logSegs = new ArrayBuffer [Int]
      logSegs += logSeg.num

      val superb = SuperBlock (
          id, boot, geometry, false, alloc.free, logSeg.num, logSeg.pos, pageSeg.num, pageSeg.limit)

      for {
        _ <- latch (
            SuperBlock.write (superb, file),
            RecordHeader.write (LogEnd, file, logSeg.pos),
            PageLedger.write (PageLedger.Zipped.empty, file, pageSeg.pos))
      } yield {
        new DiskDrive (
            id, path, file, geometry, alloc, kit, false, logSegs, logSeg.pos, logSeg.pos,
            logSeg.limit, PagedBuffer (12), pageSeg, pageSeg.limit, new PageLedger, false)
      }}}
