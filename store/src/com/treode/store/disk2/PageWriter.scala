package com.treode.store.disk2

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.async.{Callback, Scheduler, guard}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{Picklers, pickle}

private class PageWriter (
    id: Int,
    file: File,
    config: DiskDriveConfig,
    alloc: SegmentAllocator,
    scheduler: Scheduler,
    dispatcher: PageDispatcher) {

  val buffer = PagedBuffer (12)
  var base = 0L
  var pos = 0L

  class PositionCallback (id: Int, offset: Int, length: Int, cb: Callback [Position])
  extends Callback [Long] {
    def pass (base: Long) = cb (Position (id, base + offset, length))
    def fail (t: Throwable) = cb.fail (t)
  }

  def picklePage (page: PickledPage): Callback [Long] = {
    val start = buffer.writePos
    page.write (buffer)
    buffer.writeZeroToAlign (config.blockBits)
    val length = buffer.writePos - start
    new PositionCallback (id, start, length, page.cb)
  }

  val receiver: (ArrayList [PickledPage] => Unit) = (receive _)

  def receive (pages: ArrayList [PickledPage]) {

    val accepts = new ArrayList [PickledPage]
    val rejects = new ArrayList [PickledPage]
    var projection = pos
    var realloc = false
    var i = 0
    while (i < pages.size) {
      val entry = pages.get (i)
      val len = config.blockAlignLength (entry.byteSize)
      if (projection - len > 0) {
        accepts.add (entry)
        projection -= len
      } else {
        rejects.add (entry)
        realloc = true
      }
      i += 1
    }

    dispatcher.replace (rejects)

    var callbacks = new ArrayList [Callback [Long]]

    val finish = new Callback [Unit] {
      def pass (v: Unit) = {
        buffer.clear()
        val _pos = pos
        callbacks foreach (cb =>  scheduler.execute (cb (_pos)))
        dispatcher.engage (PageWriter.this)
      }
      def fail (t: Throwable) {
        buffer.clear()
        callbacks foreach (cb => scheduler.execute (cb.fail (t)))
      }}

    guard (finish) {

      for (page <- accepts)
        callbacks.add (picklePage (page))

      if (realloc) {
        val seg = alloc.allocate()
        pos = seg.limit
      } else {
        pos -= buffer.readableBytes
      }
      assert (pos == config.blockAlignPosition (pos))

      file.flush (buffer, pos, finish)
    }}

  def init (cb: Callback [Unit]) {
    val seg = alloc.allocate()
    base = seg.pos
    pos = seg.limit
    buffer.writeInt (0)
    file.flush (buffer, base, new Callback [Unit] {
      def pass (v: Unit) {
        buffer.clear()
        cb()
      }
      def fail (t: Throwable) {
        buffer.clear()
        cb.fail (t)
      }})
  }

  def checkpoint (gen: Int): PageWriter.Meta =
    PageWriter.Meta (pos)

  def recover (gen: Int, meta: PageWriter.Meta) {
    val seg = alloc.allocPos (pos)
    base = seg.pos
    pos = pos
  }}

object PageWriter {

  case class Meta (pos: Long)

  object Meta {

    val pickle = {
      import Picklers._
      wrap (long) build (Meta.apply _) inspect (_.pos)
    }}}
