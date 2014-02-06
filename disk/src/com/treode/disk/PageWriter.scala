package com.treode.disk

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.async.{Callback, Scheduler, callback, guard}
import com.treode.buffer.PagedBuffer

import PageWriter.PositionCallbackList

private class PageWriter (
    disk: DiskDrive,
    paged: PageDispatcher,
    var seg: SegmentBounds,
    var pos: Long,
    var ledger: PageLedger) {

  import disk.{id, config, file, scheduler}

  val receiver: (ArrayList [PickledPage] => Unit) = (receive _)

  def partition (pages: ArrayList [PickledPage]) = {
    val accepts = new ArrayList [PickledPage]
    val rejects = new ArrayList [PickledPage]
    val ledger = this.ledger.clone()
    var pos = this.pos
    var realloc = false
    for (page <- pages) {
      val len = config.blockAlignLength (page.byteSize)
      pos -= len
      ledger.add (page.id, page.group, len)
      if (pos - config.blockAlignLength (ledger.byteSize) > seg.pos) {
        accepts.add (page)
      } else {
        rejects.add (page)
        realloc = true
      }}
    (accepts, rejects, realloc)
  }

  def receive (pages: ArrayList [PickledPage]) {

    val (accepts, rejects, realloc) = partition (pages)
    paged.replace (rejects)

    val buffer = PagedBuffer (12)
    val callbacks = new PositionCallbackList (id)
    val ledger = new PageLedger

    for (page <- accepts) {
      val start = buffer.writePos
      page.write (buffer)
      buffer.writeZeroToAlign (config.blockBits)
      val length = buffer.writePos - start
      callbacks.add (start, length, page.cb)
      ledger.add (page.id, page.group, length)
    }

    pos -= buffer.readableBytes

    val finish = new Callback [Unit] {
      def pass (v: Unit) = {
        if (realloc) {
          disk.reallocPages (ledger, callback (callbacks) { seg =>
            val pos = PageWriter.this.pos
            PageWriter.this.seg = seg
            PageWriter.this.pos = seg.limit
            paged.engage (PageWriter.this)
            pos
          })
        } else {
          disk.advancePages (pos, ledger, callback (callbacks) { s =>
            val pos = PageWriter.this.pos
            paged.engage (PageWriter.this)
            pos
          })
        }
      }
      def fail (t: Throwable) {
        callbacks.fail (t)
      }}

    file.flush (buffer, pos, finish)
  }}

private object PageWriter {

  class PositionCallbackList (id: Int) (implicit scheduler: Scheduler) extends Callback [Long] {

    private val callbacks = new ArrayList [Callback [Long]]

    def add (offset: Int, length: Int, cb: Callback [Position]): Unit =
      callbacks.add (new Callback [Long] {

        def pass (base: Long) =
          cb (Position (id, base + offset, length))

        def fail (t: Throwable) =
          cb.fail (t)
      })

    def pass (base: Long): Unit =
      callbacks foreach (scheduler.execute (_, base))

    def fail (t: Throwable): Unit =
      callbacks foreach (scheduler.fail (_, t))
  }

}
