package com.treode.disk

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.async.{Callback, Scheduler, guard}
import com.treode.async.io.File
import com.treode.buffer.{Input, PagedBuffer, Output}
import com.treode.pickle.{Picklers, pickle}


import com.treode.pickle.{Pickler, TagRegistry}
import TagRegistry.Tagger

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
      if (projection - len > base) {
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
        if (realloc) {
          val seg = alloc.allocate()
          base = seg.pos
          pos = seg.limit
        }
        buffer.clear()
        val _pos = pos
        callbacks foreach (scheduler.execute (_, _pos))
        dispatcher.engage (PageWriter.this)
      }
      def fail (t: Throwable) {
        buffer.clear()
        callbacks foreach (scheduler.fail (_, t))
      }}

    guard (finish) {

      for (page <- accepts) {
        val start = buffer.writePos
        page.write (buffer)
        buffer.writeZeroToAlign (config.blockBits)
        val length = buffer.writePos - start
        callbacks.add (new PositionCallback (id, start, length, page.cb))
      }

      pos -= buffer.readableBytes

      file.flush (buffer, pos, finish)
    }}

  def init() {
    val seg = alloc.allocate()
    base = seg.pos
    pos = seg.limit
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
