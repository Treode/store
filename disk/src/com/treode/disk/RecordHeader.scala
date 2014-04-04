package com.treode.disk

import com.treode.async.Async
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

import Async.guard
import PageLedger.Zipped

private sealed abstract class RecordHeader

private object RecordHeader {

  case object DiskDrain extends RecordHeader
  case object LogEnd extends RecordHeader
  case class LogAlloc (next: Int) extends RecordHeader
  case class PageWrite (pos: Long, ledger: Zipped) extends RecordHeader
  case class PageClose (num: Int) extends RecordHeader
  case class SegmentFree (nums: IntSet) extends RecordHeader
  case class Entry (batch: Long, id: TypeId) extends RecordHeader
  case class Checkpoint (ledger: Zipped) extends RecordHeader

  val pickler = {
    import DiskPicklers._
    tagged [RecordHeader] (
        0x1 -> const (DiskDrain),
        0x2 -> const (LogEnd),
        0x3 -> wrap (uint) .build (LogAlloc.apply _) .inspect (_.next),
        0x4 -> wrap (ulong, pageLedger)
            .build ((PageWrite.apply _).tupled)
            .inspect (v => (v.pos, v.ledger)),
        0x5 -> wrap (uint) .build (PageClose.apply _) .inspect (_.num),
        0x6 -> wrap (intSet) .build (SegmentFree.apply _) .inspect (_.nums),
        0x7 -> wrap (fixedLong, typeId)
            .build ((Entry.apply _).tupled)
            .inspect (v => (v.batch, v.id)),
        0x8 -> wrap (pageLedger) .build (Checkpoint.apply _) .inspect (_.ledger))
  }

  val trailer = 10 // byte count, tag, next; 4 + 1 + 5

  val overhead = 19 // byte count, tag, batch, typeId; 4 + 1 + 9 + 5

  def write (entry: RecordHeader, file: File, pos: Long): Async [Unit] =
    guard {
      val buf = PagedBuffer (12)
      pickler.frame (entry, buf)
      file.flush (buf, pos)
    }}
