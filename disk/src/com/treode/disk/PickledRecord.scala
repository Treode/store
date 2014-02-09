package com.treode.disk

import com.treode.async.Callback
import com.treode.buffer.PagedBuffer

private trait PickledRecord {

  def cb: Callback [Unit]
  def time: Long
  def byteSize: Int
  def disk: Option [Int]
  def write (buf: PagedBuffer)
}

private object PickledRecord {

  private class Header (entry: RecordHeader, val disk: Option [Int], val cb: Callback [Unit])
  extends PickledRecord {
    val time = System.currentTimeMillis
    val byteSize = RecordHeader.pickler.byteSize (entry)
    def write (buf: PagedBuffer) = RecordHeader.pickler.frame (entry, buf)
    override def toString = s"PickledRecord($time, $entry)"
  }

  def apply (disk: Int, entry: RecordHeader, cb: Callback [Unit]): PickledRecord =
    new Header (entry, Some (disk), cb)

  private class Entry [R] (desc: RecordDescriptor [R], entry: R, val cb: Callback [Unit])
  extends PickledRecord {
    val time = System.currentTimeMillis
    val byteSize = RecordHeader.overhead + desc.prec.byteSize (entry)
    def disk = None
    def write (buf: PagedBuffer) = RecordRegistry.frame (desc, time, entry, buf)
    override def toString = s"PickledRecord(${desc.id}, $time, $entry)"
  }

  def apply [R] (desc: RecordDescriptor [R], entry: R, cb: Callback [Unit]): PickledRecord =
    new Entry (desc, entry, cb)
}
