package com.treode.store.simple

import com.treode.async.{AsyncIterator, Callback}

/** Preserves first cell for key and eliminates subsequent ones. */
private class OverwritesFilter private extends (SimpleCell => Boolean) {

  private var prev: SimpleCell = null

  def apply (cell: SimpleCell): Boolean = {
    if (cell == prev) {
      false
    } else {
      prev = cell
      true
    }}}

object OverwritesFilter {

  def apply (iter: AsyncIterator [SimpleCell], cb: Callback [AsyncIterator [SimpleCell]]): Unit =
    AsyncIterator.filter (iter, cb) (new OverwritesFilter)
}
