package com.treode.store.disk2

import com.treode.async.Callback
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{Pickler, pickle, size}

private trait PickledRecord {

  def id: TypeId
  def time: Long
  def cb: Callback [Unit]
  def byteSize: Int
  def write (buffer: PagedBuffer)
}

private object PickledRecord {

  def apply [A] (p: Pickler [A], _id: TypeId, _time: Long, entry: A, _cb: Callback [Unit]): PickledRecord =
    new PickledRecord {
      def id = _id
      def time = _time
      def cb = _cb
      def byteSize = size (p, entry)
      def write (buf: PagedBuffer) = pickle (p, entry, buf)
      override def toString = s"PickledRecord($id, $time, $entry)"
    }}
