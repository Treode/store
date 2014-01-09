package com.treode.store.disk2

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.async.{Callback, Scheduler}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{Picklers, pickle, unpickle}

import LogEntry.{Body, Continue, End, Header, Pending}

private class LogWriter (
    file: File,
    free: Allocator,
    scheduler: Scheduler,
    dispatcher: LogDispatcher) {

  val buffer = PagedBuffer (12)
  var head = 0L
  var pos = 0L
  var limit = 0L

  def byteSize (body: Body): Int =
    com.treode.pickle.size (Body.pickle, body)

  def pickleEntry (time: Long, body: Body) {
    val start = buffer.writePos
    buffer.writePos += Header.ByteSize
    pickle (Body.pickle, body, buffer)
    val end = buffer.writePos
    buffer.writePos = start
    pickle (Header.pickle, Header (time, end - start), buffer)
    buffer.writePos = end
  }

  val receiver: (ArrayList [Pending] => Unit) = (receive _)

  def receive (entries: ArrayList [Pending]) {

    val accepts = new ArrayList [Pending]
    val rejects = new ArrayList [Pending]
    var projection = pos
    var realloc = false
    var i = 0
    while (i < entries.size) {
      val entry = entries.get (i)
      val len = byteSize (entry.body)
      if (projection + len + LogEntry.SegmentTrailerBytes < limit) {
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

    Callback.guard (finish) {

      for (entry <- accepts)
        pickleEntry (entry.time, entry.body)
      val time = accepts .map (_.time) .max

      val _pos = pos
      if (realloc) {
        val alloc = free.allocate()
        pos = alloc.pos
        limit = alloc.limit
        pickleEntry (time, Continue (alloc.num))
      } else {
        pos += buffer.readableBytes
        pickleEntry (time, End)
      }

      file.flush (buffer, _pos, finish)
    }}

  def init (cb: Callback [Unit]) {
    val alloc = free.allocate()
    head = alloc.pos
    pos = alloc.pos
    limit = alloc.limit
    pickleEntry (System.currentTimeMillis, End)
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
    val alloc = free.allocPos (head)
    head = meta.head
    pos = head
    limit = alloc.limit
  }}

object LogWriter {

  case class Meta (head: Long)

  object Meta {

    val pickle = {
      import Picklers._
      wrap1 (long) (Meta.apply _) (_.head)
    }}}
