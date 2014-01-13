package com.treode.store.disk2

import com.treode.async.Callback
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{Pickler, pickle, size}

private trait PickledPage {

  def cb: Callback [Position]
  def byteSize: Int
  def write (buffer: PagedBuffer)
}

private object PickledPage {

  def apply [A] (p: Pickler [A], entry: A, _cb: Callback [Position]): PickledPage =
    new PickledPage {
      def cb = _cb
      def byteSize = size (p, entry)
      def write (buf: PagedBuffer) = pickle (p, entry, buf)
      override def toString = s"PickledPage($entry)"
    }}
