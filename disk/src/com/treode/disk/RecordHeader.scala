package com.treode.disk

import com.treode.async.{Callback, defer}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

import PageLedger.Zipped

private sealed abstract class RecordHeader

private object RecordHeader {

  case object DiskDrain extends RecordHeader
  case object LogEnd extends RecordHeader
  case class LogAlloc (next: Int) extends RecordHeader
  case class PageWrite (pos: Long, ledger: Zipped) extends RecordHeader
  case class PageAlloc (next: Int, ledger: Zipped) extends RecordHeader
  case class SegmentFree (nums: IntSet) extends RecordHeader
  case class Entry (time: Long, id: TypeId) extends RecordHeader

  val pickler = {
    import DiskPicklers._
    tagged [RecordHeader] (
        0x1 -> const (DiskDrain),
        0x2 -> const (LogEnd),
        0x3 -> wrap (uint) .build (LogAlloc.apply _) .inspect (_.next),
        0x4 -> wrap (ulong, pageLedger)
            .build ((PageWrite.apply _).tupled)
            .inspect (v => (v.pos, v.ledger)),
        0x5 -> wrap (uint, pageLedger)
            .build ((PageAlloc.apply _).tupled)
            .inspect (v => (v.next, v.ledger)),
        0x6 -> wrap (intSet) .build (SegmentFree.apply _) .inspect (_.nums),
        0x7 -> wrap (fixedLong, typeId)
            .build ((Entry.apply _).tupled)
            .inspect (v => (v.time, v.id)))
  }

  val trailer = 10 // byte count, tag, next; 4 + 1 + 5

  val overhead = 19 // byte count, tag, time, typeId; 4 + 1 + 9 + 5

  def write (entry: RecordHeader, file: File, pos: Long, cb: Callback [Unit]): Unit =
    defer (cb) {
      val buf = PagedBuffer (12)
      pickler.frame (entry, buf)
      file.flush (buf, pos, cb)
    }}
