package com.treode.store.disk2

import com.treode.buffer.PagedBuffer
import com.treode.pickle.{Pickler, size}

trait PickledRoot {
  def byteSize: Int
  def write (buf: PagedBuffer)
}

object PickledRoot {

  def apply [R] (p: Pickler [R], id: TypeId, root: R): PickledRoot =
    new PickledRoot {
      def byteSize = size (p, root)
      def write (buf: PagedBuffer) = RootRegistry.framer.write (p, id, root, buf)
  }}
