package com.treode.store.local.disk.timed

import com.treode.async.{AsyncIterator, Callback}
import com.treode.store.TimedCell

/** If the oldest cell for a key is a delete, then remove that cell; assumes the wrapped iterator
  * is sorted by cell.
  */
private class DeletesFilter (iter: AsyncIterator [TimedCell])
extends AsyncIterator [TimedCell] {

  private var next1: TimedCell = null
  private var next2: TimedCell = null

  private def loop (cb: Callback [Unit]) {

    if (!iter.hasNext) {

      next1 = null
      next2 = null
      cb()

    } else {

      var candidate: TimedCell = next2

      val loop = new Callback [TimedCell] {

        def pass (cell: TimedCell) {

          if (candidate == null || candidate.key != cell.key) {

            if (cell.value.isDefined) {
              next1 = cell
              next2 = null
              cb()
            } else if (!iter.hasNext) {
              next1 = null
              next2 = null
              cb()
            } else {
              candidate = cell
              iter.next (this)
            }

          } else {

            next1 = candidate
            next2 = cell
            cb()

          }}

        def fail (t: Throwable) = cb.fail (t)
      }

      iter.next (loop)

    }}

  private def init (cb: Callback [AsyncIterator [TimedCell]]) {
    loop (new Callback [Unit] {
      def pass (v: Unit): Unit = cb.apply (DeletesFilter.this)
      def fail (t: Throwable) = cb.fail (t)
    })
  }

  def hasNext: Boolean = next1 != null

  def next (cb: Callback [TimedCell]) {
    val t = next1
    if (next2 != null && next2.value.isDefined) {
      next1 = next2
      next2 = null
      cb (t)
    } else {
      loop (new Callback [Unit] {
        def pass (v: Unit): Unit = cb.apply (t)
        def fail (t: Throwable) = cb.fail (t)
      })
    }}}

private object DeletesFilter {

  def apply (iter: AsyncIterator [TimedCell], cb: Callback [AsyncIterator [TimedCell]]): Unit =
    new DeletesFilter (iter) init (cb)
}
