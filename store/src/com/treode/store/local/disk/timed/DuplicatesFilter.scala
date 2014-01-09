package com.treode.store.local.disk.timed

import com.treode.async.{AsyncIterator, Callback}
import com.treode.store.TimedCell

/** Remove cells that duplicate (key, time); assumes the wrapped iterator is sorted by cell. */
private class DuplicatesFilter private extends (TimedCell => Boolean) {

  private var prev: TimedCell = null

  def apply (cell: TimedCell): Boolean = {
    if (cell == prev) {
      false
    } else {
      prev = cell
      true
    }}}

object DuplicatesFilter {

  def apply (iter: AsyncIterator [TimedCell], cb: Callback [AsyncIterator [TimedCell]]): Unit =
    AsyncIterator.filter (iter, new DuplicatesFilter, cb)
}
