package com.treode.store.local.disk.timed

import com.treode.concurrent.Callback
import com.treode.store.{TimedCell, TxClock}
import com.treode.store.local.TimedIterator

/** Remove cells older than a limit. */
private class AgeFilter (iter: TimedIterator, limit: TxClock) extends TimedIterator {

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

  private def init (cb: Callback [TimedIterator]) {
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

  def apply (iter: TimedIterator, limit: TxClock, cb: Callback [TimedIterator]): Unit =
    new AgeFilter (iter, limit) init (cb)
}
