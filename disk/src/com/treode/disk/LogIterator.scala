package com.treode.disk

import java.nio.file.Path
import java.util.ArrayDeque

import com.treode.async._
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

private class LogIterator private (
    path: Path,
    file: File,
    buf: PagedBuffer,
    superb: SuperBlock,
    alloc: SegmentAllocator,
    pagem: PageWriter.Medic,
    records: RecordRegistry) extends AsyncIterator [(Long, Unit => Any)] {

  private val past = new ArrayDeque [Int]
  private var seg = alloc.allocPos (superb.log.head)
  private var pos = superb.log.head

  private def failed [A] (cb: Callback [A], t: Throwable) {
    pos = -1L
    cb.fail (t)
  }

  def hasNext: Boolean = pos > 0

  private def frameRead (cb: Callback [(Long, Unit => Any)]) =
    new Callback [Int] {

      def pass (len: Int) {
        val start = buf.readPos
        val hdr = RecordHeader.pickler.unpickle (buf)
        hdr match {
          case RecordHeader.End =>
            pos = -1L
            buf.clear()
            cb (Long.MaxValue, _ => ())

          case RecordHeader.Continue (num) =>
            val seg = alloc.allocSeg (num)
            pos = seg.pos
            buf.clear()
            file.deframe (buf, pos, this)

          case RecordHeader.Entry (time, id) =>
            val end = buf.readPos
            val entry = records.read (id.id, buf, len - end + start)
            pos += len
            cb (time, entry)

          case _ =>
            cb.fail (new MatchError)
        }
      }

      def fail (t: Throwable) = failed (cb, t)
    }

  def next (cb: Callback [(Long, Unit => Any)]): Unit =
    file.deframe (buf, pos, frameRead (cb))

  def close (logd: LogDispatcher, paged: PageDispatcher) (implicit scheduler: Scheduler) = {
    val logw = new LogWriter (file, alloc, logd, buf, past, seg, pos)
    val pagew = pagem.close (paged)
    new DiskDrive (superb.id, path, file, superb.config, alloc, logw, pagew)
  }}

object LogIterator {

  def apply (
      path: Path,
      file: File,
      superb: SuperBlock,
      records: RecordRegistry,
      cb: Callback [(Int, LogIterator)]) (
          implicit scheduler: Scheduler): Unit =

    guard (cb) {
      val alloc = SegmentAllocator.recover (superb.config, superb.alloc)
      PageWriter.recover (file, alloc, superb, delay (cb) { pagem =>
        val buf = PagedBuffer (12)
        file.fill (buf, superb.log.head, 4, callback (cb) { _ =>
          (superb.id, new LogIterator (path, file, buf, superb, alloc, pagem, records))
        })
      })
    }

  def replay (
      useGen1: Boolean,
      reads: Seq [SuperBlocks],
      records: RecordRegistry,
      cb: Callback [DiskDrives]) (
          implicit scheduler: Scheduler): Unit =
    guard (cb) {

      def replayed (logs: Map [Int, LogIterator]) = callback (cb) { _: Unit =>
        val logd = new LogDispatcher
        val paged = new PageDispatcher
        val disks =
          for (read <- reads) yield {
            val superb = read.superb (useGen1)
            logs (superb.id) .close (logd, paged)
          }
        new DiskDrives (logd, paged, disks.mapBy (_.id))
      }

      def merged (logs: Map [Int, LogIterator]) = delay (cb) { iter: ReplayIterator =>
        AsyncIterator.foreach (iter, replayed (logs)) { case ((time, replay), cb) =>
          guard (cb) (replay())
          cb()
        }}

      val ordering = Ordering.by [(Long, Unit => Any), Long] (_._1)

      val allMade = delay (cb) { logs: Map [Int, LogIterator] =>
        val latch = merged (logs)
        AsyncIterator.merge (logs.values.iterator, latch) (ordering)
      }

      val oneMade = Callback.map (reads.size, allMade)
      reads foreach { read =>
        val superb =
        apply (read.path, read.file, read.superb (useGen1), records, oneMade)
      }}}
