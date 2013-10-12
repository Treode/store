package com.treode.store.simple

import com.treode.cluster.concurrent.Callback

/** Preserves first cell for key and eliminates subsequent ones. */
private class OverwritesFilter private (iter: CellIterator) extends CellIterator {

  private var next: Cell = null

  private def init (cb: Callback [CellIterator]) {
    if (iter.hasNext) {
      iter.next (new Callback [Cell] {
        def apply (cell: Cell) {
          next = cell
          cb (OverwritesFilter.this)
        }
        def fail (t: Throwable) = cb.fail (t)
      })
    } else {
      cb (this)
    }}

  def hasNext: Boolean = next != null

  def next (cb: Callback [Cell]) {

    if (iter.hasNext) {

      val loop = new Callback [Cell] {

        def apply (cell: Cell) {
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

  def apply (iter: CellIterator, cb: Callback [CellIterator]): Unit =
    new OverwritesFilter (iter) init (cb)
}
