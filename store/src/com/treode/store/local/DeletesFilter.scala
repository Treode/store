package com.treode.store.local

import com.treode.cluster.concurrent.Callback
import com.treode.store.tier.{Cell, CellIterator}

/** If the oldest cell for a key is a delete, then remove that cell; assumes the wrapped iterator
  * is sorted by cell.
  */
private class DeletesFilter (iter: CellIterator) extends CellIterator {

  private var next1: Cell = null
  private var next2: Cell = null

  private def loop (cb: Callback [Unit]) {

    if (!iter.hasNext) {

      next1 = null
      next2 = null
      cb()

    } else {

      var candidate: Cell = next2

      val loop = new Callback [Cell] {

        def apply (cell: Cell) {

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

  private def init (cb: Callback [CellIterator]) {
    loop (new Callback [Unit] {
      def apply (v: Unit): Unit = cb.apply (DeletesFilter.this)
      def fail (t: Throwable) = cb.fail (t)
    })
  }

  def hasNext: Boolean = next1 != null

  def next (cb: Callback [Cell]) {
    val t = next1
    if (next2 != null && next2.value.isDefined) {
      next1 = next2
      next2 = null
      cb (t)
    } else {
      loop (new Callback [Unit] {
        def apply (v: Unit): Unit = cb.apply (t)
        def fail (t: Throwable) = cb.fail (t)
      })
    }}}

private object DeletesFilter {

  def apply (iter: CellIterator, cb: Callback [CellIterator]): Unit =
    new DeletesFilter (iter) init (cb)
}
