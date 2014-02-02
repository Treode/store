package com.treode.disk

import java.util.{ArrayDeque, ArrayList}
import scala.collection.JavaConversions._

import com.treode.async.{Callback, Scheduler, callback, guard}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

import RecordHeader.{Continue, End}

private class LogWriter (
    file: File,
    alloc: SegmentAllocator,
    logd: LogDispatcher,
    buf: PagedBuffer,
    past: ArrayDeque [Int],
    var seg: SegmentBounds,
    var pos: Long) (
        implicit scheduler: Scheduler) {

  val receiver: (ArrayList [PickledRecord] => Unit) = (receive _)

  logd.engage (this)

  def receive (entries: ArrayList [PickledRecord]) {

    val accepts = new ArrayList [PickledRecord]
    val rejects = new ArrayList [PickledRecord]
    var projection = pos
    var realloc = false
    var i = 0
    while (i < entries.size) {
      val entry = entries.get (i)
      val len = entry.byteSize
      if (projection + len + LogSegmentTrailerBytes < seg.limit) {
        accepts.add (entry)
        projection += len
      } else {
        rejects.add (entry)
        realloc = true
      }
      i += 1
    }

    logd.replace (rejects)

    val finish = new Callback [Unit] {
      def pass (v: Unit) = {
        buf.clear()
        entries foreach (e =>  scheduler.execute (e.cb, ()))
        logd.engage (LogWriter.this)
      }
      def fail (t: Throwable) {
        buf.clear()
        entries foreach (e => scheduler.execute (e.cb.fail (t)))
      }}

    guard (finish) {

      for (entry <- accepts)
        entry.write (buf)

      val time =
        if (accepts.isEmpty)
          System.currentTimeMillis
        else
          accepts.last.time

      val _pos = pos
      if (realloc) {
        past.add (seg.num)
        seg = alloc.allocate()
        pos = seg.pos
        RecordHeader.pickler.frame (Continue (seg.num), buf)
      } else {
        pos += buf.readableBytes
        RecordHeader.pickler.frame (End, buf)
      }

      file.flush (buf, _pos, finish)
    }}

  def checkpoint (gen: Int): LogWriter.Meta =
    LogWriter.Meta (pos)
}

private object LogWriter {

  def init (
      file: File,
      alloc: SegmentAllocator,
      logd: LogDispatcher,
      cb: Callback [LogWriter]) (
          implicit scheduler: Scheduler) {
    guard (cb) {
      val buf = PagedBuffer (12)
      val seg = alloc.allocate()
      RecordHeader.pickler.frame (End, buf)
      file.flush (buf, seg.pos, callback (cb) { _ =>
        new LogWriter (file, alloc, logd, buf, new ArrayDeque, seg, seg.pos)
      })
    }}

  case class Meta (head: Long)

  object Meta {

    val pickler = {
      import DiskPicklers._
      wrap (long) build (Meta.apply _) inspect (_.head)
    }}}
