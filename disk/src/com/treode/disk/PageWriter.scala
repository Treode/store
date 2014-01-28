package com.treode.disk

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.async.{Callback, Scheduler, callback, delay, guard}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

private class PageWriter (
    id: Int,
    file: File,
    config: DiskDriveConfig,
    alloc: SegmentAllocator,
    scheduler: Scheduler,
    dispatcher: PageDispatcher,
    pages: PageRegistry) {

  val segmentMap = SegmentMap.pickler (pages)
  val buffer = PagedBuffer (12)
  var base = 0L
  var pos = 0L
  var map = SegmentMap.empty

  class PositionCallback (id: Int, offset: Int, length: Int, cb: Callback [Position])
  extends Callback [Long] {
    def pass (base: Long) = cb (Position (id, base + offset, length))
    def fail (t: Throwable) = cb.fail (t)
  }

  val receiver: (ArrayList [PickledPage] => Unit) = (receive _)

  def writeMap (map: SegmentMap, pos: Long, cb: Callback [Unit]) {
    val buf = PagedBuffer (12)
    buf.writeInt (0)
    segmentMap.pickle (map, buf)
    val end = buf.writePos
    buf.writePos = 0
    buf.writeInt (end)
    buf.writePos = end
    file.flush (buf, base, cb)
  }

  def receive (pages: ArrayList [PickledPage]) {

    val accepts = new ArrayList [PickledPage]
    val rejects = new ArrayList [PickledPage]
    var projpos = pos
    var projmap = map
    var realloc = false
    var i = 0
    while (i < pages.size) {

      val entry = pages.get (i)
      val len = config.blockAlignLength (entry.byteSize)

      val tpos = projpos - len
      val tmap = map.add (entry.group, len)
      val maplen = config.blockAlignLength (tmap.byteSize)
      if (tpos - maplen > base) {
        accepts.add (entry)
        projpos -= len
        projmap = tmap
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
          writeMap (map, base, Callback.ignore)
          val seg = alloc.allocate()
          base = seg.pos
          pos = seg.limit
          map = SegmentMap.empty
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
      map = projmap

      file.flush (buffer, pos, finish)
    }}

  def init (cb: Callback [Unit]) {
    val seg = alloc.allocate()
    base = seg.pos
    pos = seg.limit
    map = SegmentMap.empty
    writeMap (map, base, cb)
  }

  def checkpoint (gen: Int, cb: Callback [Unit]): PageWriter.Meta = {
    val (map, pos, base) = (this.map, this.pos, this.base)
    writeMap (map, base, cb)
    PageWriter.Meta (pos)
  }

  def recover (gen: Int, meta: PageWriter.Meta, cb: Callback [Unit]) {
    val seg = alloc.allocPos (meta.pos)
    base = seg.pos
    pos = pos
    // TODO: Read map from disk.
    cb()
  }}

private object PageWriter {

  case class Meta (pos: Long)

  object Meta {

    val pickler = {
      import DiskPicklers._
      wrap (long) build (Meta.apply _) inspect (_.pos)
    }}}
