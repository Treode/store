package com.treode.store.disk2

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.async.{Callback, Scheduler, guard}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{Picklers, pickle, unpickle}

private class LogWriter (
    file: File,
    alloc: SegmentAllocator,
    scheduler: Scheduler,
    dispatcher: LogDispatcher) {

  val buffer = PagedBuffer (12)
  var head = 0L
  var pos = 0L
  var limit = 0L

  def pickleHeader (header: LogHeader) {
    val start = buffer.writePos
    buffer.writePos += 4
    pickle (LogHeader.pickle, header, buffer)
    val end = buffer.writePos
    val length = end - start - 4
    buffer.writePos = start
    buffer.writeInt (length)
    buffer.writePos = end
  }

  def pickleEntry (entry: PickledEntry) {
    val start = buffer.writePos
    buffer.writePos += 4
    pickle (LogHeader.pickle, LogHeader.Entry (entry.time, entry.id), buffer)
    entry.write (buffer)
    val end = buffer.writePos
    val length = end - start - 4
    buffer.writePos = start
    buffer.writeInt (length)
    buffer.writePos = end
  }

  val receiver: (ArrayList [PickledEntry] => Unit) = (receive _)

  def receive (entries: ArrayList [PickledEntry]) {

    val accepts = new ArrayList [PickledEntry]
    val rejects = new ArrayList [PickledEntry]
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
        entries foreach (e =>  scheduler.execute (e.cb()))
        dispatcher.engage (LogWriter.this)
      }
      def fail (t: Throwable) {
        buffer.clear()
        entries foreach (e => scheduler.execute (e.cb.fail (t)))
      }}

    guard (finish) {

      for (entry <- accepts)
        pickleEntry (entry)

      val time =
        if (accepts.isEmpty)
          System.currentTimeMillis
        else
          accepts .map (_.time) .max

      val _pos = pos
      if (realloc) {
        val seg = alloc.allocate()
        pos = seg.pos
        limit = seg.limit
        pickleHeader (LogHeader.Continue (seg.num))
      } else {
        pos += buffer.readableBytes
        pickleHeader (LogHeader.End)
      }

      file.flush (buffer, _pos, finish)
    }}

  def init (cb: Callback [Unit]) {
    val seg = alloc.allocate()
    head = seg.pos
    pos = seg.pos
    limit = seg.limit
    pickleHeader (LogHeader.End)
    file.flush (buffer, pos, new Callback [Unit] {
      def pass (v: Unit) {
        buffer.clear()
        cb()
      }
      def fail (t: Throwable) {
        buffer.clear()
        cb.fail (t)
      }})
  }

  def checkpoint (gen: Int): LogWriter.Meta =
    LogWriter.Meta (head)

  def recover (gen: Int, meta: LogWriter.Meta) {
    val seg = alloc.allocPos (head)
    head = meta.head
    pos = head
    limit = seg.limit
  }}

object LogWriter {

  case class Meta (head: Long)

  object Meta {

    val pickle = {
      import Picklers._
      wrap (long) build (Meta.apply _) inspect (_.head)
    }}}
