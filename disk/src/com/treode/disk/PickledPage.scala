package com.treode.disk

import com.treode.async.Callback
import com.treode.buffer.Output

private abstract class PickledPage (
    val typ: TypeId,
    val obj: ObjectId,
    val group: PageGroup,
    val cb: Callback [Position]
) {
  def byteSize: Int
  def write (out: Output)
}

private object PickledPage {

  def apply [G, P] (
      desc: PageDescriptor [G, P],
      obj: ObjectId,
      group: G,
      page: P,
      cb: Callback [Position]
  ): PickledPage =
    new PickledPage (desc.id, obj, PageGroup (desc.pgrp, group), cb) {
      def byteSize = desc.ppag.byteSize (page)
      def write (out: Output) = desc.ppag.pickle (page, out)
      override def toString = s"PickledPage($obj, $group)"
    }}
