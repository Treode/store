package com.treode.disk

import com.treode.async.Callback
import com.treode.buffer.PagedBuffer

private trait PickledRecord {

  def cb: Callback [Unit]
  def byteSize: Int
  def disk: Option [Int]
  def write (num: Long, buf: PagedBuffer)
}

private object PickledRecord {

  private class Header (entry: RecordHeader, val disk: Option [Int], val cb: Callback [Unit])
  extends PickledRecord {

    val byteSize =
      RecordHeader.pickler.byteSize (entry)

    def write (num: Long, buf: PagedBuffer) =
      RecordHeader.pickler.frame (entry, buf)

    override def toString =
      s"PickledRecord($entry)"
  }

  def apply (disk: Int, entry: RecordHeader, cb: Callback [Unit]): PickledRecord =
    new Header (entry, Some (disk), cb)

  private class Entry [R] (desc: RecordDescriptor [R], entry: R, val cb: Callback [Unit])
  extends PickledRecord {

    val byteSize =
      RecordHeader.overhead + desc.prec.byteSize (entry)

    def disk = None

    def write (num: Long, buf: PagedBuffer) =
      RecordRegistry.frame (desc, num, entry, buf)

    override def toString =
      s"PickledRecord(${desc.id}, $entry)"
  }

  def apply [R] (desc: RecordDescriptor [R], entry: R, cb: Callback [Unit]): PickledRecord =
    new Entry (desc, entry, cb)
}
