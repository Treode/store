package com.treode.store.local

import com.treode.async.Callback
import com.treode.store.SimpleCell

private trait SimpleIterator {

  def hasNext: Boolean
  def next (cb: Callback [SimpleCell])
}

private object SimpleIterator {

  def adapt (iter: Iterator [SimpleCell]): SimpleIterator =
    new SimpleIterator {
      def hasNext: Boolean = iter.hasNext
      def next (cb: Callback [SimpleCell]): Unit = cb (iter.next)
    }}
