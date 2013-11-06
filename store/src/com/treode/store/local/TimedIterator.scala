package com.treode.store.local

import com.treode.concurrent.Callback

private [store] trait TimedIterator {

  def hasNext: Boolean
  def next (cb: Callback [TimedCell])
}

private [store] object TimedIterator {

  def adapt (iter: Iterator [TimedCell]): TimedIterator =
    new TimedIterator {
      def hasNext: Boolean = iter.hasNext
      def next (cb: Callback [TimedCell]): Unit = cb (iter.next)
    }}
