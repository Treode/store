package com.treode.disk

import com.treode.async.Callback
import com.treode.buffer.Output
import com.treode.pickle.Pickler

private trait PickledPage {

  def group: PickledPageHandler
  def cb: Callback [Position]
  def byteSize: Int
  def write (out: Output)
}

private object PickledPage {

  def apply [G, P] (desc: PageDescriptor [G, P], _group: G, page: P, _cb: Callback [Position]): PickledPage =
    new PickledPage {
      def group = PickledPageHandler (desc, _group)
      def cb = _cb
      def byteSize = desc.ppag.byteSize (page)
      def write (out: Output) = desc.ppag.pickle (page, out)
      override def toString = s"PickledPage($group)"
    }}
