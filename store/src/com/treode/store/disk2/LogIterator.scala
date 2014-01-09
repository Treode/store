package com.treode.store.disk2

import com.treode.async.{AsyncIterator, Callback}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.pickle.unpickle

import LogEntry.{Body, Continue, End, Envelope, Header}

private class LogIterator private (file: File, free: Allocator)
extends AsyncIterator [Envelope] {

  private val buf = new PagedBuffer (12)
  private var pos = -1L

  private def failed [A] (cb: Callback [A], t: Throwable) {
    pos = -1L
    cb.fail (t)
  }

  private def init (pos: Long, cb: Callback [LogIterator]) {
    this.pos = pos
    file.fill (buf, pos, Header.ByteSize, new Callback [Unit] {
      def pass (v: Unit) = cb (LogIterator.this)
      def fail (t: Throwable) = failed (cb, t)
    })
  }

  def hasNext: Boolean = pos > 0

  def next (cb: Callback [Envelope]) {

    file.fill (buf, pos, Header.ByteSize, new Callback [Unit] {

      def pass (v: Unit) {
        val start = buf.readPos
        val hdr = unpickle (Header.pickle, buf)

        file.fill (buf, pos + Header.ByteSize, hdr.len - Header.ByteSize, new Callback [Unit] {

          def pass (v: Unit) {
            val body = unpickle (Body.pickle, buf)
            body match {
              case End =>
                pos = -1L
              case Continue (seg) =>
                val alloc = free.allocSeg (seg)
                pos = alloc.pos
                buf.clear()
              case _ =>
                pos += hdr.len
            }
            cb (Envelope (hdr, body))
          }

          def fail (t: Throwable) = failed (cb, t)
        })
      }

      def fail (t: Throwable) = failed (cb, t)
    })
  }}

object LogIterator {

  def apply (file: File, pos: Long, free: Allocator, cb: Callback [LogIterator]): Unit =
    new LogIterator (file, free) .init (pos, cb)
}
