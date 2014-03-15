package com.treode.disk

import java.nio.file.Path
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}

import com.treode.async.{Async, AsyncConversions, AsyncIterator, Callback, Scheduler}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

import Async.async
import AsyncConversions._
import RecordHeader.{Entry, LogAlloc, LogEnd, PageAlloc, PageWrite}

private class LogIterator private (
    records: RecordRegistry,
    path: Path,
    file: File,
    buf: PagedBuffer,
    superb: SuperBlock,
    alloc: Allocator,
    logSegs: ArrayBuffer [Int],
    private var logSeg: SegmentBounds,
    private var pageSeg: SegmentBounds,
    private var pageLedger: PageLedger
) (implicit
    scheduler: Scheduler,
    config: DisksConfig
) extends AsyncIterator [(Long, Unit => Any)] {

  private var logPos = superb.logHead
  private var pagePos = superb.pagePos

  class Foreach (f: ((Long, Unit => Any), Callback [Unit]) => Any, cb: Callback [Unit]) {

    val _read: Callback [Int] = {
      case Success (v) => read (v)
      case Failure (t) => fail (t)
    }

    val _next: Callback [Unit] = {
      case Success (v) => next()
      case Failure (t) => fail (t)
    }

    def fail (t: Throwable) {
      logPos = -1L
      cb.fail (t)
    }

    def read (len: Int) {
      val start = buf.readPos
      val hdr = RecordHeader.pickler.unpickle (buf)
      hdr match {
        case LogEnd =>
          logPos = -1L
          buf.clear()
          scheduler.pass (cb, ())

        case LogAlloc (next) =>
          logSeg = alloc.alloc (next, superb.geometry, config)
          logSegs += logSeg.num
          logPos = logSeg.pos
          buf.clear()
          file.deframe (buf, logPos) run (_read)

        case PageWrite (pos, _ledger) =>
          pagePos = pos
          pageLedger.add (_ledger)
          logPos += len + 4
          file.deframe (buf, logPos) run (_read)

        case PageAlloc (next, _ledger) =>
          pageSeg = alloc.alloc (next, superb.geometry, config)
          pagePos = pageSeg.pos
          pageLedger = _ledger.unzip
          logPos += len + 4
          file.deframe (buf, logPos) run (_read)

        case Entry (time, id) =>
          val end = buf.readPos
          val entry = records.read (id.id, buf, len - end + start)
          logPos += len + 4
          f ((time, entry), _next)

        case _ =>
          fail (new MatchError)
      }}

    def next() {
      file.deframe (buf, logPos) run (_read)
    }}

  def _foreach (f: ((Long, Unit => Any), Callback [Unit]) => Any): Async [Unit] =
    async (new Foreach (f, _) .next())

  def close (kit: DisksKit): DiskDrive = {
    buf.clear()
    val disk = new DiskDrive (
        superb.id, path, file, superb.geometry, alloc, kit, superb.draining, logSegs,
        superb.logHead, logPos, logSeg.limit, buf, pageSeg, superb.pagePos, pageLedger, false)
    disk
  }}

private object LogIterator {

  def apply (
      useGen1: Boolean,
      read: SuperBlocks,
      records: RecordRegistry
  ) (implicit
      scheduler: Scheduler,
      config: DisksConfig
  ): Async [(Int, LogIterator)] = {

    val path = read.path
    val file = read.file
    val superb = read.superb (useGen1)
    val alloc = Allocator (superb.free)
    val logSeg = alloc.alloc (superb.logSeg, superb.geometry, config)
    val logSegs = new ArrayBuffer [Int]
    logSegs += logSeg.num
    val pageSeg = alloc.alloc (superb.pageSeg, superb.geometry, config)
    val buf = PagedBuffer (12)

    for {
      ledger <- PageLedger.read (file, pageSeg.pos)
      _ <- file.fill (buf, superb.logHead, 1)
    } yield {
      val iter = new LogIterator (
          records, path, file, buf, superb, alloc, logSegs, logSeg, pageSeg, ledger)
      (superb.id, iter)
    }}

  def replay (
      useGen1: Boolean,
      reads: Seq [SuperBlocks],
      records: RecordRegistry
  ) (implicit
      scheduler: Scheduler,
      config: DisksConfig
  ): Async [DisksKit] = {

    val ordering = Ordering.by [(Long, Unit => Any), Long] (_._1)

    for {
      logs <- reads.latch.map (apply (useGen1, _, records))
      iter = AsyncIterator.merge (logs.values.toSeq) (ordering)
      _ <- iter.foreach.f (_._2())
      kit = new DisksKit
      drives =
        for (read <- reads) yield {
          val superb = read.superb (useGen1)
          logs (superb.id) .close (kit)
        }
      _ <- kit.disks.add (drives)
    } yield kit
  }}
