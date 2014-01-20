package com.treode.store.disk2

import com.treode.async.Callback
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{Pickler, pickle, size}

private trait PickledPage {

  def cb: Callback [Position]
  def byteSize: Int
  def write (buf: PagedBuffer)
}

private object PickledPage {

  def apply [A] (p: Pickler [A], page: A, _cb: Callback [Position]): PickledPage =
    new PickledPage {
      def cb = _cb
      def byteSize = size (p, page)
      def write (buf: PagedBuffer) = pickle (p, page, buf)
      override def toString = s"PickledPage($page)"
    }

  def apply (_byteSize: Int, _write: PagedBuffer => Any, _cb: Callback [Position]): PickledPage =
    new PickledPage {
      def cb = _cb
      def byteSize = _byteSize
      def write (buf: PagedBuffer) = _write (buf)
      override def toString = s"PickledPage(writer)"
  }}
