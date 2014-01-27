package com.treode.disk

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.async.{Callback, Scheduler, guard}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

import RecordHeader.{Continue, End}

private class LogWriter (
    file: File,
    alloc: SegmentAllocator,
    scheduler: Scheduler,
    dispatcher: LogDispatcher) {

  val buffer = PagedBuffer (12)
  var head = 0L
  var pos = 0L
  var limit = 0L

  val receiver: (ArrayList [PickledRecord] => Unit) = (receive _)

  def receive (entries: ArrayList [PickledRecord]) {

    val accepts = new ArrayList [PickledRecord]
    val rejects = new ArrayList [PickledRecord]
    var projection = pos
    var realloc = false
    var i = 0
    while (i < entries.size) {
      val entry = entries.get (i)
      val len = entry.byteSize
      if (projection + len + LogSegmentTrailerBytes < limit) {
        accepts.add (entry)
        projection += len
      } else {
        rejects.add (entry)
        realloc = true
      }
      i += 1
    }

    dispatcher.replace (rejects)

    val finish = new Callback [Unit] {
      def pass (v: Unit) = {
        buffer.clear()
        entries foreach (e =>  scheduler.execute (e.cb, ()))
        dispatcher.engage (LogWriter.this)
      }
      def fail (t: Throwable) {
        buffer.clear()
        entries foreach (e => scheduler.execute (e.cb.fail (t)))
      }}

    guard (finish) {

      for (entry <- accepts)
        entry.write (buffer)

      val time =
        if (accepts.isEmpty)
          System.currentTimeMillis
        else
          accepts.last.time

      val _pos = pos
      if (realloc) {
        val seg = alloc.allocate()
        pos = seg.pos
        limit = seg.limit
        RecordRegistry.framer.write (Continue (seg.num), buffer)
      } else {
        pos += buffer.readableBytes
        RecordRegistry.framer.write (End, buffer)
      }

      file.flush (buffer, _pos, finish)
    }}

  def init (cb: Callback [Unit]) {
    val seg = alloc.allocate()
    head = seg.pos
    pos = seg.pos
    limit = seg.limit
    RecordRegistry.framer.write (End, buffer)
    file.flush (buffer, pos, new Callback [Unit] {
      def pass (v: Unit) {
        buffer.clear()
        scheduler.execute (cb, ())
      }
      def fail (t: Throwable) {
        buffer.clear()
        scheduler.fail (cb, t)
      }})
  }

  def checkpoint (gen: Int): LogWriter.Meta =
    LogWriter.Meta (head)

  def recover (gen: Int, meta: LogWriter.Meta) {
    val seg = alloc.allocPos (head)
    head = meta.head
    pos = head
    limit = seg.limit
  }

  def iterator (records: RecordRegistry, cb: Callback [LogIterator]): Unit =
    LogIterator (file, head, alloc, records, cb)
}

object LogWriter {

  case class Meta (head: Long)

  object Meta {

    val pickler = {
      import DiskPicklers._
      wrap (long) build (Meta.apply _) inspect (_.head)
    }}}
