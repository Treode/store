package com.treode.store.disk2

import com.treode.async.{AsyncIterator, Callback, guard}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.pickle.unpickle

private class LogIterator private (file: File, alloc: SegmentAllocator, records: RecordRegistry)
extends AsyncIterator [Replay] {

  private val buf = new PagedBuffer (12)
  private var pos = -1L

  private def failed [A] (cb: Callback [A], t: Throwable) {
    pos = -1L
    cb.fail (t)
  }

  private def init (pos: Long, cb: Callback [LogIterator]) {
    this.pos = pos
    file.fill (buf, pos, 4, new Callback [Unit] {
      def pass (v: Unit) = cb (LogIterator.this)
      def fail (t: Throwable) = failed (cb, t)
    })
  }

  def hasNext: Boolean = pos > 0

  def readEntry (len: Int, cb: Callback [Replay]): Callback [Unit] =
    new Callback [Unit] {

      def pass (v: Unit) {
        val start = buf.readPos
        val hdr = unpickle (LogHeader.pickle, buf)
        hdr match {
          case LogHeader.End =>
            pos = -1L
            buf.clear()
            cb (Replay.noop (Long.MaxValue))

          case LogHeader.Continue (num) =>
            val seg = alloc.allocSeg (num)
            pos = seg.pos
            buf.clear()
            file.fill (buf, pos, 4, readLength (cb))

          case LogHeader.Entry (time, id) =>
            pos += len
            val replay = records.prepare (time, id, buf, len - (buf.readPos - start))
            cb (replay)
        }}

      def fail (t: Throwable) = failed (cb, t)
    }

  def readLength (cb: Callback [Replay]): Callback [Unit] =
    new Callback [Unit] {

      def pass (v: Unit) {
        pos += 4
        val len = buf.readInt()
        file.fill (buf, pos, len, readEntry (len, cb))
      }

      def fail (t: Throwable) = failed (cb, t)
    }

  def next (cb: Callback [Replay]): Unit =
    file.fill (buf, pos, 4, readLength (cb))
}

object LogIterator {

  def apply (file: File, pos: Long, free: SegmentAllocator, records: RecordRegistry,
      cb: Callback [LogIterator]): Unit =
    new LogIterator (file, free, records) .init (pos, cb)
}
