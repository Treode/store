package com.treode.disk

import com.treode.async.{AsyncIterator, Callback, delay, guard}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

private class LogIterator private (file: File, alloc: SegmentAllocator, records: RecordRegistry)
extends AsyncIterator [(Long, Unit => Any)] {

  private val buf = PagedBuffer (12)
  private var pos = -1L

  private def failed [A] (cb: Callback [A], t: Throwable) {
    pos = -1L
    cb.fail (t)
  }

  private def init (head: Long, cb: Callback [LogIterator]) {
    pos = head
    file.fill (buf, pos, 4, new Callback [Unit] {
      def pass (v: Unit) = cb (LogIterator.this)
      def fail (t: Throwable) = failed (cb, t)
    })
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
}

object LogIterator {

  def apply (file: File, head: Long, alloc: SegmentAllocator, records: RecordRegistry,
      cb: Callback [LogIterator]): Unit =
    new LogIterator (file, alloc, records) .init (head, cb)

  def replay (disks: DiskDrives, records: RecordRegistry, cb: Callback [Unit]) {

    val merged = delay (cb) { iter: ReplayIterator =>
      AsyncIterator.foreach (iter, cb) { case ((time, replay), cb) =>
        guard (cb) (replay())
        cb()
      }}

    val ordering = Ordering.by [(Long, Unit => Any), Long] (_._1)

    val allMade = delay (cb) { iters: Seq [LogIterator] =>
      AsyncIterator.merge (iters.iterator, merged) (ordering)
    }

    val oneMade = Callback.seq (disks.size, allMade)

    disks.iterator foreach (_.logIterator (records, oneMade))
  }}
