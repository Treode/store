package com.treode.disk

import com.treode.async.{Callback, defer}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

private case class SuperBlock (
    id: Int,
    boot: BootBlock,
    config: DiskDriveConfig,
    free: IntSet,
    logSeg: Int,
    logHead: Long,
    pageSeg: Int,
    pagePos: Long)

private object SuperBlock {

  val pickler = {
    import DiskPicklers._
    wrap (int, boot, config, intSet, int, long, int, long)
    .build ((SuperBlock.apply _).tupled)
    .inspect (v => (v.id, v.boot, v.config, v.free, v.logSeg, v.logHead, v.pageSeg, v.pagePos))
  }

  def position (gen: Int): Long =
    if ((gen & 0x1) == 0) 0L else SuperBlockBytes

  def write (gen: Int, superb: SuperBlock, file: File, cb: Callback [Unit]): Unit =
    defer (cb) {
      val buf = PagedBuffer (12)
      pickler.pickle (superb, buf)
      file.flush (buf, position (gen), cb)
    }}
