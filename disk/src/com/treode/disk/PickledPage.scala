package com.treode.disk

import com.treode.async.Callback
import com.treode.buffer.Output

private trait PickledPage {

  def id: TypeId
  def group: PageGroup
  def cb: Callback [Position]
  def byteSize: Int
  def write (out: Output)
}

private object PickledPage {

  def apply [G, P] (desc: PageDescriptor [G, P], _group: G, page: P, _cb: Callback [Position]): PickledPage =
    new PickledPage {
      def id: TypeId = desc.id
      def group = PageGroup (desc.pgrp, _group)
      def cb = _cb
      def byteSize = desc.ppag.byteSize (page)
      def write (out: Output) = desc.ppag.pickle (page, out)
      override def toString = s"PickledPage($group)"
    }}
