package com.treode.store.local

import com.treode.cluster.concurrent.Callback
import com.treode.store.{Cell, CellIterator, TxClock}

/** Remove cells older than a limit. */
class AgeFilter (iter: CellIterator, limit: TxClock) extends CellIterator {

  private var next: Cell = null

  private def loop (cb: Callback [Unit]) {

    if (iter.hasNext) {

      val loop = new Callback [Cell] {

        def apply (cell: Cell) {
          if (cell.time >= limit) {
            next = cell
            cb()
          } else if (!iter.hasNext) {
            next = null
            cb()
          } else {
            iter.next (this)
          }}

        def fail (t: Throwable) = cb.fail (t)
      }

      iter.next (loop)

    } else {
      next = null
      cb()
    }}

  private def init (cb: Callback [CellIterator]) {
    loop (new Callback [Unit] {
      def apply (v: Unit): Unit = cb (AgeFilter.this)
      def fail (t: Throwable) = cb.fail (t)
    })
  }

  def hasNext: Boolean = next != null

  def next (cb: Callback [Cell]) {
    val t = next
    loop (new Callback [Unit] {
      def apply (v: Unit): Unit = cb (t)
      def fail (t: Throwable) = cb.fail (t)
    })
  }}

private object AgeFilter {

  def apply (iter: CellIterator, limit: TxClock, cb: Callback [CellIterator]): Unit =
    new AgeFilter (iter, limit) init (cb)
}
