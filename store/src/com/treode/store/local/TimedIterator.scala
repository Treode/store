package com.treode.store.local

import com.treode.async.Callback
import com.treode.store.TimedCell

private trait TimedIterator {

  def hasNext: Boolean
  def next (cb: Callback [TimedCell])
}

private object TimedIterator {

  def adapt (iter: Iterator [TimedCell]): TimedIterator =
    new TimedIterator {
      def hasNext: Boolean = iter.hasNext
      def next (cb: Callback [TimedCell]): Unit = cb (iter.next)
    }}
