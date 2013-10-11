package com.treode.store

import com.treode.cluster.concurrent.Callback

trait CellIterator {

  def hasNext: Boolean
  def next (cb: Callback [Cell])
}

object CellIterator {

  def adapt (iter: Iterator [Cell]): CellIterator =
    new CellIterator {
      def hasNext: Boolean = iter.hasNext
      def next (cb: Callback [Cell]): Unit = cb (iter.next)
    }}
