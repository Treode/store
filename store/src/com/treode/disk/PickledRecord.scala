package com.treode.disk

import com.treode.async.Callback
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{Pickler, pickle, size}

private trait PickledRecord {

  def cb: Callback [Unit]
  def time: Long
  def byteSize: Int
  def write (buffer: PagedBuffer)
}

private object PickledRecord {

  def apply [A] (p: Pickler [A], id: TypeId, _time: Long, entry: A, _cb: Callback [Unit]): PickledRecord =
    new PickledRecord {
      val hdr = RecordHeader.Entry (_time, id)
      def cb = _cb
      def time = _time
      val byteSize = 17 + size (p, entry) // TODO: Yikes! A magic number...
      def write (buf: PagedBuffer) =
        RecordRegistry.framer.write (p, hdr, entry, buf)
      override def toString = s"PickledRecord($id, $time, $entry)"
    }}
