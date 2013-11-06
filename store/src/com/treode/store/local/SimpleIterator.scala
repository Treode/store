package com.treode.store.local

import com.treode.concurrent.Callback

private [store] trait SimpleIterator {

  def hasNext: Boolean
  def next (cb: Callback [SimpleCell])
}

private [store] object SimpleIterator {

  def adapt (iter: Iterator [SimpleCell]): SimpleIterator =
    new SimpleIterator {
      def hasNext: Boolean = iter.hasNext
      def next (cb: Callback [SimpleCell]): Unit = cb (iter.next)
    }}
