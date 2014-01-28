package com.treode.disk

import com.treode.async.Callback
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{Pickler, Picklers, PicklerRegistry}

private trait PickledRecord {

  def cb: Callback [Unit]
  def time: Long
  def byteSize: Int
  def write (buffer: PagedBuffer)
}

private object PickledRecord {

  def apply [R] (desc: RecordDescriptor [R], _time: Long, entry: R, _cb: Callback [Unit]): PickledRecord =
    new PickledRecord {
      def cb = _cb
      def time = _time
      val byteSize = 17 + desc.prec.byteSize (entry) // TODO: Yikes! A magic number...
      def write (buf: PagedBuffer) = RecordRegistry.frame (desc, time, entry, buf)
      override def toString = s"PickledRecord(${desc.id}, $time, $entry)"
    }}
