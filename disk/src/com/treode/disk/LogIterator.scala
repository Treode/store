package com.treode.disk

import java.nio.file.Path
import java.util.ArrayDeque
import scala.util.{Failure, Success}

import com.treode.async.{Async, AsyncIterator, Callback, Scheduler}
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
    buf: PagedBuffer,
    superb: SuperBlock,
    alloc: Allocator,
    logSegs: ArrayDeque [Int],
    private var logSeg: SegmentBounds
) (implicit
    scheduler: Scheduler,
    config: DiskConfig
) extends AsyncIterator [(Long, Unit => Any)] {

  private var draining = superb.draining
  private var logPos = superb.logHead
  private var logTail = -1L
  private var pagePos = Option.empty [Long]
  private var pageLedger = new PageLedger

  class Foreach (f: ((Long, Unit => Any)) => Async [Unit], cb: Callback [Unit]) {

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
      val start = buf.readPos
      val hdr = RecordHeader.pickler.unpickle (buf)
      hdr match {

        case LogEnd =>
          logTail = logPos
          logPos = -1L
          buf.clear()
          scheduler.pass (cb, ())

        case LogAlloc (next) =>
          logSeg = alloc.alloc (next, superb.geometry, config)
          logSegs.add (logSeg.num)
          logPos = logSeg.base
          buf.clear()
          file.deframe (buf, logPos) run (_read)

        case PageWrite (pos, _ledger) =>
          val num = superb.geometry.segmentNum (pos)
          alloc.alloc (num, superb.geometry, config)
          pagePos = Some (pos)
          pageLedger.add (_ledger)
          logPos += len + 4
          file.deframe (buf, logPos) run (_read)

        case PageClose (num) =>
          alloc.alloc (num, superb.geometry, config)
          pagePos = None
          pageLedger = new PageLedger
          logPos += len + 4
          file.deframe (buf, logPos) run (_read)

        case SegmentFree (nums) =>
          alloc.free (nums)
          logPos += len + 4
          file.deframe (buf, logPos) run (_read)

        case Checkpoint (pos, _ledger) =>
          val num = superb.geometry.segmentNum (pos - 1)
          alloc.alloc (num, superb.geometry, config)
          pagePos = Some (pos)
          pageLedger = _ledger.unzip
          logPos += len + 4
          file.deframe (buf, logPos) run (_read)

        case DiskDrain (num) =>
          draining = true
          pagePos = None
          pageLedger = new PageLedger
          logPos += len + 4
          file.deframe (buf, logPos) run (_read)

        case Entry (batch, id) =>
          val end = buf.readPos
          val entry = records.read (id.id, buf, len - end + start)
          logPos += len + 4
          f ((batch, entry)) run (_next)
      }}

    def next() {
      file.deframe (buf, logPos) run (_read)
    }}

  def foreach (f: ((Long, Unit => Any)) => Async [Unit]): Async [Unit] =
    async (new Foreach (f, _) .next())

  def pages(): (SegmentBounds, Long, PageLedger, Boolean) =
    pagePos match {
      case _ if draining =>
        val seg = SegmentBounds (-1, -1L, -1L)
        (seg, -1L, new PageLedger, false)
      case Some (pos) =>
        val num = superb.geometry.segmentNum (pos - 1)
        val seg = alloc.alloc (num, superb.geometry, config)
        (seg, pos, pageLedger, true)
      case None =>
        val seg = alloc.alloc (superb.geometry, config)
        (seg, seg.limit, pageLedger, true)
    }

  def close (kit: DiskKit): DiskDrive = {
    val (seg, head, ledger, dirty) = pages()
    new DiskDrive (
        superb.id, path, file, superb.geometry, alloc, kit, draining, logSegs,
        superb.logHead, logTail, logSeg.limit, buf, seg, head, ledger, dirty)
  }}

private object LogIterator {

  def apply (
      useGen0: Boolean,
      read: SuperBlocks,
      records: RecordRegistry
  ) (implicit
      scheduler: Scheduler,
      config: DiskConfig
  ): Async [(Int, LogIterator)] = {

    val path = read.path
    val file = read.file
    val superb = read.superb (useGen0)
    val alloc = Allocator (superb.free)
    val num = superb.geometry.segmentNum (superb.logHead)
    val logSeg = alloc.alloc (num, superb.geometry, config)
    val logSegs = new ArrayDeque [Int]
    logSegs.add (logSeg.num)
    val buf = PagedBuffer (12)

    for {
      _ <- file.fill (buf, superb.logHead, 1)
    } yield {
      val iter =
          new LogIterator (records, path, file, buf, superb, alloc, logSegs, logSeg)
      (superb.id, iter)
    }}

  def replay (
      reads: Seq [SuperBlocks],
      records: RecordRegistry
  ) (implicit
      scheduler: Scheduler,
      config: DiskConfig
  ): Async [DiskKit] = {

    val ordering = Ordering.by [(Long, Unit => Any), Long] (_._1)
    val useGen0 = chooseSuperBlock (reads)
    var logBatch = 0L

    def replay (_entry: (Long, Unit => Any)) {
      val (batch, entry) = _entry
      logBatch = batch
      entry()
    }

    for {
      logs <- reads.latch.map foreach (apply (useGen0, _, records))
      iter = AsyncIterator.merge (logs.values.toSeq) (ordering, scheduler)
      _ <- iter.foreach (entry => supply (replay (entry)))
      kit = new DiskKit (logBatch)
      drives =
        for (read <- reads) yield {
          val superb = read.superb (useGen0)
          logs (superb.id) .close (kit)
        }
      _ <- kit.disks.add (drives)
    } yield kit
  }}
