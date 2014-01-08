package com.treode.store.local.disk.timed

import com.treode.async.{AsyncIterator, Callback}
import com.treode.store.{TimedCell, TxClock}

/** Remove cells older than a limit. */
private class AgeFilter (iter: AsyncIterator [TimedCell], limit: TxClock)
extends AsyncIterator [TimedCell] {

  private var next: TimedCell = null

  private def loop (cb: Callback [Unit]) {

    if (iter.hasNext) {

      val loop = new Callback [TimedCell] {

        def pass (cell: TimedCell) {
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

  private def init (cb: Callback [AsyncIterator [TimedCell]]) {
    loop (new Callback [Unit] {
      def pass (v: Unit): Unit = cb (AgeFilter.this)
      def fail (t: Throwable) = cb.fail (t)
    })
  }

  def hasNext: Boolean = next != null

  def next (cb: Callback [TimedCell]) {
    val t = next
    loop (new Callback [Unit] {
      def pass (v: Unit): Unit = cb (t)
      def fail (t: Throwable) = cb.fail (t)
    })
  }}

private object AgeFilter {

  def apply (iter: AsyncIterator [TimedCell], limit: TxClock, cb: Callback [AsyncIterator [TimedCell]]): Unit =
    new AgeFilter (iter, limit) init (cb)
}
