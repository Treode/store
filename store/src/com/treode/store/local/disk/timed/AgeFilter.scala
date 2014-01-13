package com.treode.store.local.disk.timed

import com.treode.async.{AsyncIterator, Callback}
import com.treode.store.{TimedCell, TxClock}

/** Remove cells older than a limit. */
private object AgeFilter {

  def apply (iter: AsyncIterator [TimedCell], limit: TxClock, cb: Callback [AsyncIterator [TimedCell]]): Unit =
    AsyncIterator.filter (iter, cb) (_.time >= limit)
}
