package com.treode.store.local.disk.simple

import com.treode.concurrent.Callback
import com.treode.store.local.{SimpleCell, SimpleIterator}

/** Preserves first cell for key and eliminates subsequent ones. */
private class OverwritesFilter private (iter: SimpleIterator) extends SimpleIterator {

  private var next: SimpleCell = null

  private def init (cb: Callback [SimpleIterator]) {
    if (iter.hasNext) {
      iter.next (new Callback [SimpleCell] {
        def pass (cell: SimpleCell) {
          next = cell
          cb (OverwritesFilter.this)
        }
        def fail (t: Throwable) = cb.fail (t)
      })
    } else {
      cb (this)
    }}

  def hasNext: Boolean = next != null

  def next (cb: Callback [SimpleCell]) {

    if (iter.hasNext) {

      val loop = new Callback [SimpleCell] {

        def pass (cell: SimpleCell) {
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

object OverwritesFilter {

  def apply (iter: SimpleIterator, cb: Callback [SimpleIterator]): Unit =
    new OverwritesFilter (iter) init (cb)
}
