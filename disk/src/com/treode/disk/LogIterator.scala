package com.treode.disk

import java.nio.file.Path
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}

import com.treode.async.{Async, AsyncConversions, AsyncIterator, Callback, Scheduler}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

import Async.async
import AsyncConversions._
import RecordHeader._
import SuperBlocks.chooseSuperBlock

private class LogIterator private (
    records: RecordRegistry,
    path: Path,
    file: File,
    buf: PagedBuffer,
    superb: SuperBlock,
    alloc: Allocator,
    logSegs: ArrayBuffer [Int],
    private var logSeg: SegmentBounds
) (implicit
    scheduler: Scheduler,
    config: DisksConfig
) extends AsyncIterator [(Long, Unit => Any)] {

  private var draining = superb.draining
  private var logPos = superb.logHead
  private var pagePos = Option.empty [Long]
  private var pageLedger = new PageLedger

  class Foreach (f: ((Long, Unit => Any), Callback [Unit]) => Any, cb: Callback [Unit]) {

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

        case DiskDrain =>
          draining = true
          logPos += len + 4
          file.deframe (buf, logPos) run (_read)

        case LogEnd =>
          logPos = -1L
          buf.clear()
          scheduler.pass (cb, ())

        case PageEnd =>
          pagePos = None
          pageLedger = new PageLedger
          logPos += len + 4
          file.deframe (buf, logPos) run (_read)

        case LogAlloc (next) =>
          logSeg = alloc.alloc (next, superb.geometry, config)
          logSegs += logSeg.num
          logPos = logSeg.pos
          buf.clear()
          file.deframe (buf, logPos) run (_read)

        case PageWrite (pos, _ledger) =>
          val num = (pos >> superb.geometry.segmentBits) .toInt
          val seg = alloc.alloc (num, superb.geometry, config)
          pagePos = Some (pos)
          pageLedger.add (_ledger)
          logPos += len + 4
          file.deframe (buf, logPos) run (_read)

        case SegmentFree (nums) =>
          alloc.free (nums)
          logPos += len + 4
          file.deframe (buf, logPos) run (_read)

        case Entry (batch, id) =>
          val end = buf.readPos
          val entry = records.read (id.id, buf, len - end + start)
          logPos += len + 4
          f ((batch, entry), _next)
      }}

    def next() {
      file.deframe (buf, logPos) run (_read)
    }}

  def _foreach (f: ((Long, Unit => Any), Callback [Unit]) => Any): Async [Unit] =
    async (new Foreach (f, _) .next())

  def close (kit: DisksKit): DiskDrive = {
    buf.clear()
    val (seg, pos) = pagePos match {
      case Some (pos) =>
        val num = (pos >> superb.geometry.segmentBits) .toInt
        val seg = alloc.alloc (num, superb.geometry, config)
        (seg, pos)
      case None =>
        val seg = alloc.alloc (superb.geometry, config)
        (seg, seg.limit)
    }
    val disk = new DiskDrive (
        superb.id, path, file, superb.geometry, alloc, kit, draining, logSegs,
        superb.logHead, logPos, logSeg.limit, buf, seg, pos, pageLedger, false)
    disk
  }}

private object LogIterator {

  def apply (
      useGen0: Boolean,
      read: SuperBlocks,
      records: RecordRegistry
  ) (implicit
      scheduler: Scheduler,
      config: DisksConfig
  ): Async [(Int, LogIterator)] = {

    val path = read.path
    val file = read.file
    val superb = read.superb (useGen0)
    val alloc = Allocator (superb.free)
    val logSeg = alloc.alloc (superb.logSeg, superb.geometry, config)
    val logSegs = new ArrayBuffer [Int]
    logSegs += logSeg.num
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
      config: DisksConfig
  ): Async [DisksKit] = {

    val ordering = Ordering.by [(Long, Unit => Any), Long] (_._1)
    val useGen0 = chooseSuperBlock (reads)
    var logBatch = 0L

    def replay (batch: Long, entry: Unit => Any) {
      logBatch = batch
      entry()
    }

    for {
      logs <- reads.latch.map foreach (apply (useGen0, _, records))
      iter = AsyncIterator.merge (logs.values.toSeq) (ordering)
      _ <- iter.foreach.f ((replay _).tupled)
      kit = new DisksKit (logBatch)
      drives =
        for (read <- reads) yield {
          val superb = read.superb (useGen0)
          logs (superb.id) .close (kit)
        }
      _ <- kit.disks.add (drives)
    } yield kit
  }}
