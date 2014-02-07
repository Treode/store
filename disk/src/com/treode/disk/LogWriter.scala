package com.treode.disk

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.async.{Callback, Scheduler, callback, defer}
import com.treode.buffer.PagedBuffer

private class LogWriter (
    disk: DiskDrive,
    logd: LogDispatcher,
    var buf: PagedBuffer,
    var seg: SegmentBounds,
    var pos: Long) (
        implicit scheduler: Scheduler) {

  val receiver: (ArrayList [PickledRecord] => Unit) = (receive _)

  def partition (entries: ArrayList [PickledRecord]) = {
    val accepts = new ArrayList [PickledRecord]
    val rejects = new ArrayList [PickledRecord]
    var pos = this.pos
    var realloc = false
    var i = 0
    for (entry <- entries) {
      if (entry.disk.isDefined && entry.disk.get != disk.id) {
        rejects.add (entry)
      } else if (pos + entry.byteSize + RecordHeader.trailer < seg.limit) {
        accepts.add (entry)
        pos += entry.byteSize
      } else {
        rejects.add (entry)
        realloc = true
      }}
    (accepts, rejects, realloc)
  }

  def receive (entries: ArrayList [PickledRecord]) {

    val (accepts, rejects, realloc) = partition (entries)
    logd.replace (rejects)

    val finish = new Callback [Unit] {

      def pass (v: Unit) = {
        buf.clear()
        logd.engage (LogWriter.this)
        accepts foreach (e =>  scheduler.execute (e.cb, ()))
      }

      def fail (t: Throwable) {
        buf.clear()
        logd.engage (LogWriter.this)
        accepts foreach (e => scheduler.execute (e.cb.fail (t)))
      }}

    defer (finish) {

      for (entry <- accepts)
        entry.write (buf)

      val time =
        if (accepts.isEmpty)
          System.currentTimeMillis
        else
          accepts.last.time

      val _pos = pos
      if (realloc) {
        disk.reallocLog (buf, callback (finish) { seg =>
          this.seg = seg
          pos = seg.pos
          buf.clear()
        })
      } else {
        pos += buf.readableBytes
        disk.advanceLog (buf, finish)
      }}}}
