package com.treode.disk

import java.nio.file.Path
import java.util.{ArrayDeque, ArrayList, PriorityQueue}
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

import com.treode.async._
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.pickle.Pickler

import RecordHeader.{LogAlloc, LogEnd, PageAlloc, PageWrite}

private class DiskDrive (
    val id: Int,
    val path: Path,
    val file: File,
    val geometry: DiskGeometry,
    val alloc: SegmentAllocator,
    val disks: DiskDrives,
    var logSegs: ArrayDeque [Int],
    var logHead: Long,
    var logTail: Long,
    var logLimit: Long,
    var logBuf: PagedBuffer,
    var pageSeg: SegmentBounds,
    var pageHead: Long,
    var pageLedger: PageLedger
) {
  import disks.{config, logd, paged, scheduler}

  private val LogPriority = 1
  private val PagePriority = 2
  private val CheckpointPriority = 3

  private val fiber = new Fiber (scheduler)
  private val queue = new PriorityQueue [(Int, Runnable)]
  private var state: State = Ready
  private var pageLedgerDirty = false

  trait State {
    def advanceLog (cb: Callback [Unit])
    def reallocLog (cb: Callback [Unit])
    def advancePages (pos: Long, ledger: PageLedger, cb: Callback [Unit])
    def reallocPages (ledger: PageLedger, cb: Callback [Unit])
    def checkpoint (boot: BootBlock, cb: Callback [Unit])
  }

  private def ready(): Unit = fiber.execute {
    try {
      state = Ready
      val task = queue.poll()
      if (task != null)
        task._2.run()
    } catch {
      case t: Throwable =>
        ready()
        throw t
    }}

  private def ready [A, B] (cb: Callback [A], v: A): Callback [B] =
    new Callback [B] {
      def pass (vb: B) {
        ready()
        scheduler.pass (cb, v)
      }
      def fail (t: Throwable) {
        ready()
        scheduler.fail (cb, t)
      }}

  private def enque (priority: Int, task: Runnable): Unit =
    queue.add ((priority, task))

  private def enque (priority: Int) (f: => Any): Unit =
    enque (priority, Scheduler.toRunnable (f))

  private def _advanceLog (cb: Callback [Unit]): Unit =
    defer (cb) {
      val len = logBuf.readableBytes
      RecordHeader.pickler.frame (LogEnd, logBuf)
      file.flush (logBuf, logTail, callback (cb) { _ =>
        logTail += len
        logBuf.clear()
      })
    }

  private def _reallocLog (cb: Callback [Unit]): Unit =
    defer (cb) {
      println ("realloc log")
      state = ReallocLog
      val buf1 = PagedBuffer (12)
      val seg = alloc.alloc()
      RecordHeader.pickler.frame (LogAlloc (seg.num), logBuf)
      RecordHeader.pickler.frame (LogEnd, buf1)
      val _cb = ready (cb, ())
      file.flush (buf1, seg.pos, continue (_cb) { _ =>
        file.flush (logBuf, logTail, callback (_cb) { _ =>
          logSegs.add (seg.num)
          logTail = seg.pos
          logLimit = seg.limit
          logBuf.clear()
        })
      })
    }

  private def _advancePages (pos: Long, ledger: PageLedger, cb: Callback [Unit]): Unit =
    defer (cb) {
      pageHead = pos
      pageLedger.add (ledger)
      pageLedgerDirty = true
      disks.record (id, PageWrite (pos, ledger.zip), cb)
    }

  private def _reallocPages (ledger: PageLedger, cb: Callback [Unit]): Unit =
    defer (cb) {
      val s0 = pageSeg
      val s1 = alloc.alloc()
      state = ReallocPages
      pageSeg = s1
      pageHead = s1.limit
      pageLedger.add (ledger)
      pageLedgerDirty = false
      val _cb = ready (cb, ())
      PageLedger.write (pageLedger, file, s0.pos, continue (_cb) { _: Unit =>
        PageLedger.write (PageLedger.Zipped.empty, file, s1.pos, continue (_cb) { _: Unit =>
          disks.record (id, PageAlloc (s1.num, ledger.zip), _cb)
        })
      })
    }

  private def _checkpoint (boot: BootBlock, cb: Callback [Unit]): Unit =
    defer (cb) {
      state = Checkpoint
      val superb =
        SuperBlock (id, boot, geometry, alloc.free, logSegs.peek, logHead, pageSeg.num, pageHead)
      val _cb = ready (cb, ())
      if (pageLedgerDirty) {
        pageLedgerDirty = false
        PageLedger.write (pageLedger, file, pageSeg.pos, continue (_cb) { _: Unit =>
          SuperBlock.write (boot.gen, superb, file, _cb)
        })
      } else {
        SuperBlock.write (boot.gen, superb, file, _cb)
      }}

  object Ready extends State {

    def advanceLog (cb: Callback [Unit]): Unit =
      _advanceLog (cb)

    def reallocLog (cb: Callback [Unit]): Unit =
      _reallocLog (cb)

    def advancePages (pos: Long, ledger: PageLedger, cb: Callback [Unit]): Unit =
      _advancePages (pos, ledger, cb)

    def reallocPages (ledger: PageLedger, cb: Callback [Unit]): Unit =
      _reallocPages (ledger, cb)

    def checkpoint (boot: BootBlock, cb: Callback [Unit]): Unit =
      _checkpoint (boot, cb)
  }

  object ReallocLog extends State {

    def advanceLog (cb: Callback [Unit]): Unit =
      cb.fail (new IllegalStateException)

    def reallocLog (cb: Callback [Unit]): Unit =
      cb.fail (new IllegalStateException)

    def advancePages (pos: Long, ledger: PageLedger, cb: Callback [Unit]): Unit =
      _advancePages (pos, ledger, cb)

    def reallocPages (ledger: PageLedger, cb: Callback [Unit]): Unit =
      enque (PagePriority) (state.reallocPages (ledger, cb))

    def checkpoint (boot: BootBlock, cb: Callback [Unit]): Unit =
      enque (CheckpointPriority) (state.checkpoint (boot, cb))
  }

  object Checkpoint extends State {

    def advanceLog (cb: Callback [Unit]): Unit =
      _advanceLog (cb)

    def reallocLog (cb: Callback [Unit]): Unit =
      enque (LogPriority) (state.reallocLog (cb))

    def advancePages (pos: Long, ledger: PageLedger, cb: Callback [Unit]): Unit =
      _advancePages (pos, ledger, cb)

    def reallocPages (ledger: PageLedger, cb: Callback [Unit]): Unit =
      enque (PagePriority) (state.reallocPages (ledger, cb))

    def checkpoint (boot: BootBlock, cb: Callback [Unit]): Unit =
      cb.fail (new IllegalStateException ("Checkpoint already in progres"))
  }

  object ReallocPages extends State {

    def advanceLog (cb: Callback [Unit]): Unit =
      _advanceLog (cb)

    def reallocLog (cb: Callback [Unit]): Unit =
      enque (LogPriority) (state.reallocLog (cb))

    def advancePages (pos: Long, ledger: PageLedger, cb: Callback [Unit]): Unit =
      cb.fail (new IllegalStateException)

    def reallocPages (ledger: PageLedger, cb: Callback [Unit]): Unit =
      cb.fail (new IllegalStateException)

    def checkpoint (boot: BootBlock, cb: Callback [Unit]): Unit =
      enque (CheckpointPriority) (state.checkpoint (boot, cb))
  }

  def advanceLog (cb: Callback [Unit]): Unit =
    fiber.execute (state.advanceLog (cb))

  def reallocLog (cb: Callback [Unit]): Unit =
    fiber.execute (state.reallocLog (cb))

  def advancePages (pos: Long, ledger: PageLedger, cb: Callback [Unit]): Unit =
    fiber.execute (state.advancePages (pos, ledger, cb))

  def reallocPages (ledger: PageLedger, cb: Callback [Unit]): Unit =
    fiber.execute (state.reallocPages (ledger, cb))

  def checkpoint (boot: BootBlock, cb: Callback [Unit]): Unit =
    fiber.execute (state.checkpoint (boot, cb))

  def mark (cb: Callback [Unit]): Unit = fiber.execute {
    invoke (cb) {
      logHead = logTail
    }}

  def cleanable (cb: Callback [(DiskDrive, IntSet)]): Unit = fiber.execute {
    defer (cb) {
      val skip = new ArrayBuffer [Int] (logSegs.size + 1)
      skip ++= logSegs
      skip += pageSeg.num
      cb (this, alloc.cleanable (skip))
    }}

  def free (segs: Seq [Int]): Unit = fiber.execute {
    alloc.free (segs)
  }

  def splitRecords (entries: ArrayList [PickledRecord]) = {
    val accepts = new ArrayList [PickledRecord]
    val rejects = new ArrayList [PickledRecord]
    var pos = logHead
    var realloc = false
    for (entry <- entries) {
      if (entry.disk.isDefined && entry.disk.get != id) {
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

  def writeRecords (entries: ArrayList [PickledRecord]) = {
    val callbacks = new ArrayList [Callback [Unit]]
    for (entry <- entries) {
      entry.write (logBuf)
      callbacks.add (entry.cb)
    }
    callbacks
  }

  def receiveRecords (entries: ArrayList [PickledRecord]) {

    val (accepts, rejects, realloc) = splitRecords (entries)
    logd.replace (rejects)

    val callbacks = writeRecords (accepts)
    val flushed = Callback.fanout (callbacks, scheduler)

    disks.tallyLog (logBuf.readableBytes, accepts.size)
    if (realloc) {
      reallocLog (callback (flushed) { _ =>
        logd.receive (recordReceiver)
      })
    } else {
      advanceLog (callback (flushed) { _ =>
        logd.receive (recordReceiver)
      })
    }}

  val recordReceiver: ArrayList [PickledRecord] => Unit = (receiveRecords _)

  def splitPages (pages: ArrayList [PickledPage]) = {
    val projector = pageLedger.project
    val limit = (pageHead - pageSeg.pos).toInt
    val accepts = new ArrayList [PickledPage]
    val rejects = new ArrayList [PickledPage]
    var totalBytes = 0
    var realloc = false
    for (page <- pages) {
      projector.add (page.id, page.group)
      val pageBytes = geometry.blockAlignLength (page.byteSize)
      val ledgerBytes = geometry.blockAlignLength (projector.byteSize)
      if (ledgerBytes + pageBytes < limit) {
        accepts.add (page)
        totalBytes += pageBytes
      } else {
        rejects.add (page)
        realloc = true
      }}
    (accepts, rejects, realloc)
  }

  def writePages (pages: ArrayList [PickledPage]) = {
    val buffer = PagedBuffer (12)
    val callbacks = new ArrayList [Callback [Long]]
    val ledger = new PageLedger
    for (page <- pages) {
      val start = buffer.writePos
      page.write (buffer)
      buffer.writeZeroToAlign (geometry.blockBits)
      val length = buffer.writePos - start
      callbacks.add (DiskDrive.offset (id, start, length, page.cb))
      ledger.add (page.id, page.group, length)
    }
    (buffer, callbacks, ledger)
  }

  def receivePages (pages: ArrayList [PickledPage]) {

    val (accepts, rejects, realloc) = splitPages (pages)
    paged.replace (rejects)

    val (buffer, callbacks, ledger) = writePages (pages)
    val pos = pageHead - buffer.readableBytes
    val logged = Callback.fanout (callbacks, scheduler)

    val flushed = continue (logged) { _: Unit =>
      if (realloc) {
        disks.tallyPages (1)
        reallocPages (ledger, callback (logged) { _ =>
          paged.receive (pageReceiver)
          pos
        })
      } else {
        advancePages (pos, ledger, callback (logged) { _ =>
          paged.receive (pageReceiver)
          pos
        })
      }}

    file.flush (buffer, pos, flushed)
  }

  val pageReceiver: ArrayList [PickledPage] => Unit = (receivePages _)

  def read [P] (desc: PageDescriptor [_, P], pos: Position, cb: Callback [P]): Unit =
    DiskDrive.read (file, desc, pos, cb)

  def close() = file.close()

  override def hashCode: Int = id.hashCode

  override def equals (other: Any): Boolean =
    other match {
      case that: DiskDrive => id == that.id
      case _ => false
    }

  override def toString = s"DiskDrive($id, $path)"
}

private object DiskDrive {

  def offset (id: Int, offset: Long, length: Int, cb: Callback [Position]): Callback [Long] =
    new Callback [Long] {
      def pass (base: Long) = cb (Position (id, base + offset, length))
      def fail (t: Throwable) = cb.fail (t)
    }

  def read [P] (file: File, desc: PageDescriptor [_, P], pos: Position, cb: Callback [P]): Unit =
    defer (cb) {
      val buf = PagedBuffer (12)
      file.fill (buf, pos.offset, pos.length, callback (cb) { _ =>
        desc.ppag.unpickle (buf)
      })
    }

  def init (
      id: Int,
      path: Path,
      file: File,
      geometry: DiskGeometry,
      boot: BootBlock,
      disks: DiskDrives,
      cb: Callback [DiskDrive]
  ): Unit =

    defer (cb) {
      import disks.{config, logd, paged}

      val alloc = SegmentAllocator.init (geometry)
      val logSeg = alloc.alloc()
      val pageSeg = alloc.alloc()
      val superb =
        SuperBlock (id, boot, geometry, alloc.free, logSeg.num, logSeg.pos,
            pageSeg.num, pageSeg.limit)

      val latch = Latch.unit (3, callback (cb) { _: Unit =>
        val logSegs = new ArrayDeque [Int]
        logSegs.add (logSeg.num)
        val disk =
          new DiskDrive (id, path, file, geometry, alloc, disks, logSegs, logSeg.pos,
              logSeg.pos, logSeg.limit, PagedBuffer (12), pageSeg, pageSeg.limit, new PageLedger)
        logd.receive (disk.recordReceiver)
        paged.receive (disk.pageReceiver)
        disk
      })

      SuperBlock.write (0, superb, file, latch)
      RecordHeader.write (LogEnd, file, logSeg.pos, latch)
      PageLedger.write (PageLedger.Zipped.empty, file, pageSeg.pos, latch)
    }

  def init (
      items: Seq [(Path, File, DiskGeometry)],
      base: Int,
      boot: BootBlock,
      disks: DiskDrives,
      cb: Callback [Seq [DiskDrive]]
  ): Unit =

    defer (cb) {
      val latch = Latch.seq (items.size, cb)
      for (((path, file, geometry), i) <- items zipWithIndex) {
        DiskDrive.init (base + i, path, file, geometry, boot, disks, latch)
      }}
}
