package com.treode.store.local.disk.timed

import com.treode.async.{AsyncIterator, Callback}
import com.treode.store.TimedCell

/** Remove cells that duplicate (key, time); assumes the wrapped iterator is sorted by cell. */
private class DuplicatesFilter private (iter: AsyncIterator [TimedCell])
extends AsyncIterator [TimedCell] {

  private var next: TimedCell = null

  private def init (cb: Callback [AsyncIterator [TimedCell]]) {
    if (iter.hasNext) {
      iter.next (new Callback [TimedCell] {
        def pass (cell: TimedCell) {
          next = cell
          cb (DuplicatesFilter.this)
        }
        def fail (t: Throwable) = cb.fail (t)
      })
    } else {
      cb (this)
    }}

  def hasNext: Boolean = next != null

  def next (cb: Callback [TimedCell]) {

    if (iter.hasNext) {

      val loop = new Callback [TimedCell] {

        def pass (cell: TimedCell) {
          if (next != cell) {
            val t = next
            next = cell
            cb (t)
          } else if (!iter.hasNext) {
            val t = next
            next = null
            cb (t)
          } else {
            iter.next (this)
          }}

        def fail (t: Throwable) = cb.fail (t)
      }

      iter.next (loop)

    } else {
      val t = next
      next = null
      cb (t)
    }}}

object DuplicatesFilter {

  def apply (iter: AsyncIterator [TimedCell], cb: Callback [AsyncIterator [TimedCell]]): Unit =
    new DuplicatesFilter (iter) init (cb)
}
