package com.treode.disk

import java.nio.file.Path
import java.util.{Arrays, ArrayDeque}
import java.util.concurrent.Executors
import scala.collection.JavaConversions._
import scala.collection.mutable.UnrolledBuffer
import scala.util.{Failure, Success}

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
    val geometry: DiskGeometry,
    val alloc: Allocator,
    val kit: DiskKit,
    var draining: Boolean,
    var logSegs: ArrayDeque [Int],
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

  val fiber = new Fiber
  val logmp = new Multiplexer [PickledRecord] (kit.logd)
  val logr: (Long, UnrolledBuffer [PickledRecord]) => Unit = (receiveRecords _)
  val pagemp = new Multiplexer [PickledPage] (kit.paged)
  val pager: (Long, UnrolledBuffer [PickledPage]) => Unit = (receivePages _)
  var compacting = IntSet()

  def record (entry: RecordHeader): Async [Unit] =
    async (cb => logmp.send (PickledRecord (id, entry, cb)))

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
      while (! (geometry.segmentBounds (logSegs.peek) .contains (logHead)))
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
      val superb = SuperBlock (id, boot, geometry, draining, alloc.free, logHead)
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

  private def _cleanable: Iterator [SegmentPointer] = {
    val skip = _protected.add (compacting)
    for (seg <- alloc .cleanable (skip) .iterator)
      yield SegmentPointer (this, geometry.segmentBounds (seg))
  }

  def cleanable(): Async [Iterator [SegmentPointer]] =
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
      val nums = IntSet (seg.num)
      assert (!(_protected contains seg.num))
      compacting = compacting.remove (nums)
      alloc.free (nums)
      record (SegmentFree (nums)) run (ignore)
      if (draining && alloc.drained (_protected))
        disks.detach (this)
    }

  private def _writeLedger(): Async [Unit] =
    when (pageLedgerDirty) {
      pageLedgerDirty = false
      PageLedger.write (pageLedger.clone(), file, pageSeg.base, pageHead)
    }

  def drain(): Async [Iterator [SegmentPointer]] =
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

  def detach(): Unit =
    fiber.guard {
      for (_ <- logmp.close())
        yield file.close()
    } run (ignore)

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
    val callbacks = new UnrolledBuffer [Callback [Unit]]
    for (entry <- entries) {
      entry.write (batch, buf)
      callbacks.add (entry.cb)
    }
    callbacks
  }

  private def reallocRecords(): Async [Unit] = {
    val newBuf = PagedBuffer (12)
    val newSeg = alloc.alloc (geometry, config)
    logSegs.add (newSeg.num)
    RecordHeader.pickler.frame (LogEnd, newBuf)
    RecordHeader.pickler.frame (LogAlloc (newSeg.num), logBuf)
    for {
      _ <- file.flush (newBuf, newSeg.base)
      _ <- file.flush (logBuf, logTail)
    } yield fiber.execute {
      logTail = newSeg.base
      logLimit = newSeg.limit
      logBuf.clear()
      logmp.receive (logr)
    }}

  private def advanceRecords(): Async [Unit] = {
    val len = logBuf.readableBytes
    assert (logTail + len <= logLimit)
    RecordHeader.pickler.frame (LogEnd, logBuf)
    for {
      _ <- file.flush (logBuf, logTail)
    } yield fiber.execute {
      logTail += len
      logBuf.clear()
      logmp.receive (logr)
    }}

  def receiveRecords (batch: Long, entries: UnrolledBuffer [PickledRecord]): Unit =
    fiber.execute {

      val (accepts, rejects, realloc) = splitRecords (entries)
      logmp.replace (rejects)

      val callbacks = writeRecords (logBuf, batch, accepts)
      val cb = fanout (callbacks)
      assert (logTail + logBuf.readableBytes <= logLimit)

      checkpointer.tally (logBuf.readableBytes, accepts.size)

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
      _ <- PageLedger.write (pageLedger, file, pageSeg.base, pageHead)
      _ <- record (PageClose (pageSeg.num))
    } yield fiber.execute {
      pageSeg = alloc.alloc (geometry, config)
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
      pagemp.replace (rejects)

      val (buffer, callbacks, ledger) = writePages (accepts)
      val pos = pageHead - buffer.readableBytes
      assert (pos >= pageSeg.base + geometry.blockBytes)
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

  override def toString = s"DiskDrive($path, $geometry)"
}

private object DiskDrive {

  def offset (id: Int, offset: Long, length: Int, cb: Callback [Position]): Callback [Long] =
    cb.continue (base => Some (Position (id, base + offset, length)))

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
      kit: DiskKit
  ): Async [DiskDrive] =

    guard {
      import kit.config

      val alloc = Allocator (geometry, config)
      val logSeg = alloc.alloc (geometry, config)
      val logSegs = new ArrayDeque [Int]
      logSegs.add (logSeg.num)
      val pageSeg = alloc.alloc (geometry, config)

      val superb = SuperBlock (id, boot, geometry, false, alloc.free, logSeg.base)

      for {
        _ <- latch (
            SuperBlock.write (superb, file),
            RecordHeader.write (LogEnd, file, logSeg.base))
      } yield {
        new DiskDrive (
            id, path, file, geometry, alloc, kit, false, logSegs, logSeg.base, logSeg.base,
            logSeg.limit, PagedBuffer (12), pageSeg, pageSeg.limit, new PageLedger, true)
       }}

  def init (
      id: Int,
      path: Path,
      file: File,
      geometry: DiskGeometry,
      boot: BootBlock
  ) (implicit
      config: DiskConfig
  ): Async [Unit] =
    guard {

      val alloc = Allocator (geometry, config)
      val logSeg = alloc.alloc (geometry, config)
      val logSegs = new ArrayDeque [Int]
      logSegs.add (logSeg.num)
      val pageSeg = alloc.alloc (geometry, config)

      val superb = SuperBlock (id, boot, geometry, false, alloc.free, logSeg.base)

      for {
        _ <- latch (
            SuperBlock.write (superb, file) (config),
            RecordHeader.write (LogEnd, file, logSeg.base))
      } yield ()
    }

  def init (
      sysid: Array [Byte],
      items: Seq [(Path, DiskGeometry)]
  ) (implicit
      scheduler: Scheduler,
      config: DiskConfig
  ): Async [Unit] =
    guard {
      val attaching = items.map (_._1) .toSet
      require (!items.isEmpty, "Must list at least one file or device to initialize.")
      require (attaching.size == items.size, "Cannot initialize a path multiple times.")
      val boot = BootBlock.apply (sysid, 0, items.size, attaching)
      for (((path, geom), id) <- items.zipWithIndex.latch.unit) {
        val file = openFile (path, geom)
        init (id, path, file, geom, boot) .ensure (file.close())
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
    try {
      implicit val scheduler = Scheduler (executor)
      implicit val config = DiskConfig.suggested (superBlockBits = superBlockBits)
      val geom = DiskGeometry (segmentBits, blockBits, diskBytes)
      val items = paths map (path => (path, geom))
      init (sysid, items) .await()
    } finally {
      executor.shutdown()
    }}}
