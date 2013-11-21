package com.treode.store.local.disk.timed

import com.treode.async.Callback
import com.treode.store.TimedCell
import com.treode.store.local.TimedIterator

/** Remove cells that duplicate (key, time); assumes the wrapped iterator is sorted by cell. */
private class DuplicatesFilter private (iter: TimedIterator) extends TimedIterator {

  private var next: TimedCell = null

  private def init (cb: Callback [TimedIterator]) {
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

  def apply (iter: TimedIterator, cb: Callback [TimedIterator]): Unit =
    new DuplicatesFilter (iter) init (cb)
}
