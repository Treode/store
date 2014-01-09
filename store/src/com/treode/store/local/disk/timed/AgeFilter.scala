package com.treode.store.local.disk.timed

import com.treode.async.{AsyncIterator, Callback}
import com.treode.store.{TimedCell, TxClock}

/** Remove cells older than a limit. */
private class AgeFilter private (limit: TxClock) extends (TimedCell => Boolean) {

  def apply (cell: TimedCell): Boolean =
    cell.time >= limit
}

private object AgeFilter {

  def apply (iter: AsyncIterator [TimedCell], limit: TxClock, cb: Callback [AsyncIterator [TimedCell]]): Unit =
    AsyncIterator.filter (iter, new AgeFilter (limit), cb)
}
