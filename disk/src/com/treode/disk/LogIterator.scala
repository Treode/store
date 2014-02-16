package com.treode.disk

import java.nio.file.Path
import scala.collection.mutable.ArrayBuffer

import com.treode.async._
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

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

    val _read = new Callback [Int] {
      def pass (v: Int): Unit = read (v)
      def fail (t: Throwable): Unit = Foreach.this.fail (t)
    }

    val _next = new Callback [Unit] {
      def pass (v: Unit): Unit = next()
      def fail (t: Throwable): Unit = Foreach.this.fail (t)
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
          scheduler.execute (cb, ())

        case LogAlloc (next) =>
          logSeg = alloc.alloc (next, superb.geometry, config)
          logSegs += logSeg.num
          logPos = logSeg.pos
          buf.clear()
          file.deframe (buf, logPos, _read)

        case PageWrite (pos, _ledger) =>
          pagePos = pos
          pageLedger.add (_ledger)
          logPos += len + 4
          file.deframe (buf, logPos, _read)

        case PageAlloc (next, _ledger) =>
          pageSeg = alloc.alloc (next, superb.geometry, config)
          pagePos = pageSeg.pos
          pageLedger = _ledger.unzip
          logPos += len + 4
          file.deframe (buf, logPos, _read)

        case Entry (time, id) =>
          val end = buf.readPos
          val entry = records.read (id.id, buf, len - end + start)
          logPos += len + 4
          f ((time, entry), _next)

        case _ =>
          fail (new MatchError)
      }}

    def next() {
      file.deframe (buf, logPos, _read)
    }}

  def foreach (cb: Callback [Unit]) (f: ((Long, Unit => Any), Callback [Unit]) => Any): Unit =
    new Foreach (f, cb) .next()

  def close (disks: DiskDrives, logd: LogDispatcher, paged: PageDispatcher): DiskDrive = {
    buf.clear()
    val disk = new DiskDrive (
        superb.id, path, file, superb.geometry, alloc, disks, superb.draining, logSegs,
        superb.logHead, logPos, logSeg.limit, buf, pageSeg, superb.pagePos, pageLedger, false)
    disk
  }}

object LogIterator {

  def apply (
      path: Path,
      file: File,
      superb: SuperBlock,
      records: RecordRegistry,
      cb: Callback [(Int, LogIterator)]
  ) (implicit
      scheduler: Scheduler,
      config: DisksConfig): Unit =

    defer (cb) {

      val alloc = Allocator (superb.free)
      val logSeg = alloc.alloc (superb.logSeg, superb.geometry, config)
      val logSegs = new ArrayBuffer [Int]
      logSegs += logSeg.num
      val pageSeg = alloc.alloc (superb.pageSeg, superb.geometry, config)
      val buf = PagedBuffer (12)
      PageLedger.read (file, pageSeg.pos, continue (cb) { ledger =>
        file.fill (buf, superb.logHead, 1, callback (cb) { _ =>
          val iter = new LogIterator (
              records, path, file, buf, superb, alloc, logSegs, logSeg, pageSeg, ledger)
          (superb.id, iter)
        })
      })
    }

  def replay (
      useGen1: Boolean,
      reads: Seq [SuperBlocks],
      records: RecordRegistry,
      cb: Callback [DiskDrives]
  ) (implicit
      scheduler: Scheduler,
      config: DisksConfig
  ): Unit =

    defer (cb) {

      def added (disks: DiskDrives) = callback (cb) { _: Unit =>
        disks
      }

      def replayed (logs: Map [Int, LogIterator]) = continue (cb) { _: Unit =>
        val disks = new DiskDrives
        val drives =
          for (read <- reads) yield {
            val superb = read.superb (useGen1)
            logs (superb.id) .close (disks, disks.logd, disks.paged)
          }
        disks.add (drives, added (disks))
      }

      val ordering = Ordering.by [(Long, Unit => Any), Long] (_._1)

      val allMade = continue (cb) { logs: Map [Int, LogIterator] =>
        val iter = AsyncIterator.merge (logs.values.toSeq) (ordering)
        iter.foreach (replayed (logs)) { case ((time, replay), cb) =>
          invoke (cb) (replay())
        }}

      val oneMade = Latch.map (reads.size, allMade)
      reads foreach { read =>
        val superb =
        apply (read.path, read.file, read.superb (useGen1), records, oneMade)
      }}}
