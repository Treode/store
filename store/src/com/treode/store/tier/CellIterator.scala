package com.treode.store.tier

import com.treode.concurrent.Callback

private [store] trait CellIterator {

  def hasNext: Boolean
  def next (cb: Callback [Cell])
}

private [store] object CellIterator {

  def adapt (iter: Iterator [Cell]): CellIterator =
    new CellIterator {
      def hasNext: Boolean = iter.hasNext
      def next (cb: Callback [Cell]): Unit = cb (iter.next)
    }}
