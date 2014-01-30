package com.treode.disk

import com.treode.async.{Callback, callback}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

import SegmentMap.entryBytes

private class SegmentMap (
    private val map: Map [(TypeId, PageGroup), Long],
    val byteSize: Int) {

  def add [P] (id: TypeId, group: PageGroup, pageBytes: Int): SegmentMap = {
    map.get ((id, group)) match {
      case Some (totalBytes) =>
          new SegmentMap (
              map + ((id, group) -> (totalBytes + pageBytes)),
              byteSize)

      case None =>
        new SegmentMap (
            map + ((id, group) -> pageBytes),
            byteSize + entryBytes + group.byteSize)
    }}

  def groups: Map [TypeId, Set [PageGroup]] =
    map.keys.groupBy (_._1) .mapValues (_.map (_._2) .toSet)

  def probe (pages: PageRegistry, cb: Callback [Long]) {

    val pagesProbed = callback (cb) { liveGroups: Map [TypeId, Set [PageGroup]] =>
      var liveBytes = 0L
      for {
        (id, pageGroups) <- liveGroups
        group <- pageGroups
      } liveBytes += map ((id, group))
      liveBytes
    }

    val groupsByType = map.keys.groupBy (_._1) .mapValues (_.map (_._2) .toSet)
    val latch = Callback.map (groupsByType.size, pagesProbed)
    for ((id, groups) <- groupsByType)
      pages.probe (id, groups, latch)
  }}

private object SegmentMap {

  val intBytes = 5
  val longBytes = 9
  val overheadBytes = intBytes         // count of entries
  val entryBytes = intBytes+longBytes  // typeId, count of bytes

  val empty = new SegmentMap (Map.empty, overheadBytes)

  val pickler = {
    import DiskPicklers._
    tagged [SegmentMap] (
        0x1 -> wrap (tuple (map (tuple (typeId, pageGroup), long), int))
               .build (v => new SegmentMap (v._1, v._2))
               .inspect (v => (v.map, v.byteSize)))
  }

  def read (file: File, pos: Long, cb: Callback [SegmentMap]) {
    val buf = PagedBuffer (12)
    file.deframe (buf, pos, callback (cb) { _ =>
      pickler.unpickle (buf)
    })
  }

  def write (file: File, pos: Long, map: SegmentMap, cb: Callback [Unit]) {
    val buf = PagedBuffer (12)
    pickler.frame (map, buf)
    file.flush (buf, pos, cb)
  }}
