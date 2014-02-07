package com.treode.disk

import java.nio.file.Path
import java.util.{ArrayDeque, PriorityQueue}
import scala.collection.JavaConversions._
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
    val config: DiskDriveConfig,
    val alloc: SegmentAllocator,
    val logd: LogDispatcher,
    var logSegs: ArrayDeque [Int],
    var logHead: Long,
    var logTail: Long,
    var pageSeg: SegmentBounds,
    var pagePos: Long,
    var pageLedger: PageLedger) (
        implicit val scheduler: Scheduler) {

  private val LogPriority = 1
  private val PagePriority = 2
  private val CheckpointPriority = 3

  private val fiber = new Fiber (scheduler)
  private val queue = new PriorityQueue [(Int, Runnable)]
  private var state: State = Ready
  private var pageLedgerDirty = false

  trait State {
    def advanceLog (buf: PagedBuffer, cb: Callback [Unit])
    def reallocLog (buf: PagedBuffer, cb: Callback [SegmentBounds])
    def advancePages (pos: Long, ledger: PageLedger, cb: Callback [Unit])
    def reallocPages (ledger: PageLedger, cb: Callback [SegmentBounds])
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
        scheduler.execute (cb, v)
      }
      def fail (t: Throwable) {
        ready()
        scheduler.fail (cb, t)
      }}

  private def enque (priority: Int, task: Runnable): Unit =
    queue.add ((priority, task))

  private def enque (priority: Int) (f: => Any): Unit =
    enque (priority, toRunnable (f))

  private def _advanceLog (buf: PagedBuffer, cb: Callback [Unit]): Unit =
    defer (cb) {
      val len = buf.readableBytes
      RecordHeader.pickler.frame (LogEnd, buf)
      file.flush (buf, logTail, callback (cb) { _ =>
        logTail += len
      })
    }

  private def _reallocLog (buf0: PagedBuffer, cb: Callback [SegmentBounds]): Unit =
    defer (cb) {
      state = ReallocLog
      val buf1 = PagedBuffer (12)
      val seg = alloc.alloc()
      RecordHeader.pickler.frame (LogAlloc (seg.num), buf0)
      RecordHeader.pickler.frame (LogEnd, buf1)
      val _cb = ready (cb, seg)
      file.flush (buf1, seg.pos, continue (_cb) { _ =>
        file.flush (buf0, logTail, callback (_cb) { _ =>
          logSegs.add (seg.num)
          logTail = seg.pos
        })
      })
    }

  private def _advancePages (pos: Long, ledger: PageLedger, cb: Callback [Unit]): Unit =
    defer (cb) {
      pagePos = pos
      pageLedger.add (ledger)
      pageLedgerDirty = true
      logd.record (id, PageWrite (pos, ledger.zip), cb)
    }

  private def _reallocPages (ledger: PageLedger, cb: Callback [SegmentBounds]): Unit =
    defer (cb) {
      val s0 = pageSeg
      val s1 = alloc.alloc()
      state = ReallocPages
      pageSeg = s1
      pagePos = s1.limit
      pageLedger.add (ledger)
      pageLedgerDirty = false
      val _cb = ready (cb, s1)
      PageLedger.write (pageLedger, file, s0.pos, continue (_cb) { _: Unit =>
        PageLedger.write (PageLedger.Zipped.empty, file, s1.pos, continue (_cb) { _: Unit =>
          logd.record (id, PageAlloc (s1.num, ledger.zip), _cb)
        })
      })
    }

  private def _mark (cb: Callback [Unit]): Unit =
    invoke (cb) {
      logHead = logTail
    }

  private def _checkpoint (boot: BootBlock, cb: Callback [Unit]): Unit =
    defer (cb) {
      state = Checkpoint
      val superb = SuperBlock (id, boot, config, alloc.free, logSegs.last, logTail,
          pageSeg.num, pagePos)
      val _cb = ready (cb, ())
      if (pageLedgerDirty) {
        pageLedgerDirty = false
        PageLedger.write (pageLedger, file, pageSeg.pos, continue (_cb) { _: Unit =>
          SuperBlock.write (boot.gen, superb, file, _cb)
        })
      } else {
        SuperBlock.write (boot.gen, superb, file, _cb)
      }}

  private def _allocated (cb: Callback [IntSet]): Unit =
    defer (cb) {
      cb (alloc.allocated)
    }

  object Ready extends State {

    def advanceLog (buf: PagedBuffer, cb: Callback [Unit]): Unit =
      _advanceLog (buf, cb)

    def reallocLog (buf: PagedBuffer, cb: Callback [SegmentBounds]): Unit =
      _reallocLog (buf, cb)

    def advancePages (pos: Long, ledger: PageLedger, cb: Callback [Unit]): Unit =
      _advancePages (pos, ledger, cb)

    def reallocPages (ledger: PageLedger, cb: Callback [SegmentBounds]): Unit =
      _reallocPages (ledger, cb)

    def checkpoint (boot: BootBlock, cb: Callback [Unit]): Unit =
      _checkpoint (boot, cb)
  }

  object ReallocLog extends State {

    def advanceLog (buf: PagedBuffer, cb: Callback [Unit]): Unit =
      cb.fail (new IllegalStateException)

    def reallocLog (buf0: PagedBuffer, cb: Callback [SegmentBounds]): Unit =
      cb.fail (new IllegalStateException)

    def advancePages (pos: Long, ledger: PageLedger, cb: Callback [Unit]): Unit =
      _advancePages (pos, ledger, cb)

    def reallocPages (ledger: PageLedger, cb: Callback [SegmentBounds]): Unit =
      enque (PagePriority) (state.reallocPages (ledger, cb))

    def checkpoint (boot: BootBlock, cb: Callback [Unit]): Unit =
      enque (CheckpointPriority) (state.checkpoint (boot, cb))
  }

  object Checkpoint extends State {

    def advanceLog (buf: PagedBuffer, cb: Callback [Unit]): Unit =
      _advanceLog (buf, cb)

    def reallocLog (buf0: PagedBuffer, cb: Callback [SegmentBounds]): Unit =
      enque (LogPriority) (state.reallocLog (buf0, cb))

    def advancePages (pos: Long, ledger: PageLedger, cb: Callback [Unit]): Unit =
      _advancePages (pos, ledger, cb)

    def reallocPages (ledger: PageLedger, cb: Callback [SegmentBounds]): Unit =
      enque (PagePriority) (state.reallocPages (ledger, cb))

    def checkpoint (boot: BootBlock, cb: Callback [Unit]): Unit =
      cb.fail (new IllegalStateException ("Checkpoint already in progres"))
  }

  object ReallocPages extends State {

    def advanceLog (buf: PagedBuffer, cb: Callback [Unit]): Unit =
      _advanceLog (buf, cb)

    def reallocLog (buf: PagedBuffer, cb: Callback [SegmentBounds]): Unit =
      enque (LogPriority) (state.reallocLog (buf, cb))

    def advancePages (pos: Long, ledger: PageLedger, cb: Callback [Unit]): Unit =
      cb.fail (new IllegalStateException)

    def reallocPages (ledger: PageLedger, cb: Callback [SegmentBounds]): Unit =
      cb.fail (new IllegalStateException)

    def checkpoint (boot: BootBlock, cb: Callback [Unit]): Unit =
      enque (CheckpointPriority) (state.checkpoint (boot, cb))
  }

  def advanceLog (buf: PagedBuffer, cb: Callback [Unit]): Unit =
    fiber.execute (state.advanceLog (buf, cb))

  def reallocLog (buf: PagedBuffer, cb: Callback [SegmentBounds]): Unit =
    fiber.execute (state.reallocLog (buf, cb))

  def advancePages (pos: Long, ledger: PageLedger, cb: Callback [Unit]): Unit =
    fiber.execute (state.advancePages (pos, ledger, cb))

  def reallocPages (ledger: PageLedger, cb: Callback [SegmentBounds]): Unit =
    fiber.execute (state.reallocPages (ledger, cb))

  def mark (cb: Callback [Unit]): Unit =
    fiber.execute (_mark (cb))

  def checkpoint (boot: BootBlock, cb: Callback [Unit]): Unit =
    fiber.execute (state.checkpoint (boot, cb))

  def allocated (cb: Callback [IntSet]): Unit =
    fiber.execute (_allocated (cb))

  def read [P] (desc: PageDescriptor [_, P], pos: Position, cb: Callback [P]): Unit =
    DiskDrive.read (file, desc, pos, cb)

  def close() = file.close()

  override def hashCode: Int = path.hashCode

  override def equals (other: Any): Boolean =
    other match {
      case that: DiskDrive => path == that.path
      case _ => false
    }

  override def toString = s"DiskDrive($id, $path)"
}

private object DiskDrive {

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
      config: DiskDriveConfig,
      boot: BootBlock,
      logd: LogDispatcher,
      paged: PageDispatcher,
      cb: Callback [DiskDrive]) (
          implicit scheduler: Scheduler): Unit =

    defer (cb) {

      val alloc = SegmentAllocator.init (config)
      val logSeg = alloc.alloc()
      val pageSeg = alloc.alloc()
      val superb =
        SuperBlock (id, boot, config, alloc.free, logSeg.num, logSeg.pos,
            pageSeg.num, pageSeg.limit)

      val latch = Callback.latch (3, callback (cb) { _: Unit =>
        val logSegs = new ArrayDeque [Int]
        logSegs.add (logSeg.num)
        val disk =
          new DiskDrive (id, path, file, config, alloc, logd, logSegs, logSeg.pos, logSeg.pos,
              pageSeg, pageSeg.limit, new PageLedger)
        val logw =
          new LogWriter (disk, logd, PagedBuffer (12), logSeg, logSeg.pos)
        logd.engage (logw)
        val pagew =
          new PageWriter (disk, paged, pageSeg, pageSeg.limit, new PageLedger)
        paged.engage (pagew)
        disk
      })

      SuperBlock.write (0, superb, file, latch)
      RecordHeader.write (LogEnd, file, logSeg.pos, latch)
      PageLedger.write (PageLedger.Zipped.empty, file, pageSeg.pos, latch)
    }

  def init (items: Seq [(Path, File, DiskDriveConfig)], base: Int, boot: BootBlock,
      logd: LogDispatcher, paged: PageDispatcher, cb: Callback [Seq [DiskDrive]]) (
          implicit scheduler: Scheduler): Unit =

    defer (cb) {
      val latch = Callback.seq (items.size, cb)
      for (((path, file, config), i) <- items zipWithIndex) {
        DiskDrive.init (base + i, path, file, config, boot, logd, paged, latch)
      }}
}
