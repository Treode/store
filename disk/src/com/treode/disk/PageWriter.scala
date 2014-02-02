package com.treode.disk

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.async.{Callback, Scheduler, callback, guard}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.pickle.Pickler

private class PageWriter (
    id: Int,
    file: File,
    config: DiskDriveConfig,
    alloc: SegmentAllocator,
    paged: PageDispatcher,
    var base: Long,
    var pos: Long,
    var map: SegmentMap) (
        implicit scheduler: Scheduler) {

  val buffer = PagedBuffer (12)

  val receiver: (ArrayList [PickledPage] => Unit) = (receive _)

  paged.engage (this)

  class PositionCallback (id: Int, offset: Int, length: Int, cb: Callback [Position])
  extends Callback [Long] {
    def pass (base: Long) = cb (Position (id, base + offset, length))
    def fail (t: Throwable) = cb.fail (t)
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
      val tmap = map.add (entry.id, entry.group, len)
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

    paged.replace (rejects)

    var callbacks = new ArrayList [Callback [Long]]

    val finish = new Callback [Unit] {
      def pass (v: Unit) = {
        if (realloc) {
          val seg = alloc.allocate()
          base = seg.pos
          pos = seg.limit
          map = SegmentMap.empty
          SegmentMap.write (file, base, map, Callback.ignore)
        }
        buffer.clear()
        val _pos = pos
        callbacks foreach (scheduler.execute (_, _pos))
        paged.engage (PageWriter.this)
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

  def checkpoint (gen: Int, cb: Callback [Unit]): PageWriter.Meta = {
    SegmentMap.write (file, base, map, cb)
    PageWriter.Meta (base, pos)
  }}

private object PageWriter {

  case class Meta (base: Long, pos: Long)

  object Meta {

    val pickler = {
      import DiskPicklers._
      wrap (long, long)
      .build ((Meta.apply _).tupled)
      .inspect (v => (v.base, v.pos))
    }}

  class Medic (file: File, alloc: SegmentAllocator, superb: SuperBlock, var map: SegmentMap) (
      implicit scheduler: Scheduler) {

    var seg = alloc.allocPos (superb.pages.pos)
    var base = seg.pos
    var pos = superb.pages.pos

    def close (paged: PageDispatcher) =
      new PageWriter (superb.id, file, superb.config, alloc, paged, base, pos, map)
  }

  def init (
      id: Int,
      file: File,
      config: DiskDriveConfig,
      alloc: SegmentAllocator,
      paged: PageDispatcher) (
          implicit scheduler: Scheduler): PageWriter = {
    val seg = alloc.allocate()
    new PageWriter (id, file, config, alloc, paged, seg.pos, seg.limit, SegmentMap.empty)
  }

  def recover (file: File, alloc: SegmentAllocator, superb: SuperBlock, cb: Callback [Medic]) (
      implicit scheduler: Scheduler): Unit =
    guard (cb) {
      val seg = alloc.allocPos (superb.pages.base)
      val buf = PagedBuffer (12)
      SegmentMap.read (file, seg.pos, callback (cb) { map =>
        new Medic (file, alloc, superb, map)
      })
    }}
