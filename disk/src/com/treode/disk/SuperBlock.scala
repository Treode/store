package com.treode.disk

import com.treode.async.{Callback, defer}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

private case class SuperBlock (
    id: Int,
    boot: BootBlock,
    geometry: DiskGeometry,
    free: IntSet,
    logSeg: Int,
    logHead: Long,
    pageSeg: Int,
    pagePos: Long)

private object SuperBlock {

  val pickler = {
    import DiskPicklers._
    wrap (uint, boot, geometry, intSet, uint, ulong, uint, ulong)
    .build ((SuperBlock.apply _).tupled)
    .inspect (v => (v.id, v.boot, v.geometry, v.free, v.logSeg, v.logHead, v.pageSeg, v.pagePos))
  }

  def position (gen: Int) (implicit config: DisksConfig): Long =
    if ((gen & 0x1) == 0) 0L else config.superBlockBytes

  def write (gen: Int, superb: SuperBlock, file: File, cb: Callback [Unit]) (implicit config: DisksConfig): Unit =
    defer (cb) {
      val buf = PagedBuffer (12)
      pickler.pickle (superb, buf)
      file.flush (buf, position (gen), cb)
    }}
