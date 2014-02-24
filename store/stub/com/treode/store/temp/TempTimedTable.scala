package com.treode.store.temp

import java.util.concurrent.ConcurrentSkipListSet

import com.treode.async.Callback
import com.treode.store._

private class TempTimedTable extends TimedTable {

  // Visible for testing.
  protected val memtable = new ConcurrentSkipListSet [TimedCell] (TimedCell)

  def get (key: Bytes, time: TxClock, cb: Callback [TimedCell]): Unit =
    cb.defer {
      val c = memtable.ceiling (TimedCell (key, time, None))
      if (c == null)
        cb.pass (TimedCell (key, TxClock.zero, None))
      else
        cb.pass (c)
    }

  def put (key: Bytes, time: TxClock, value: Option [Bytes], cb: Callback [Unit]): Unit =
    cb.defer {
      memtable.add (TimedCell (key, time, value))
      cb.pass()
    }

  def close() = ()
}
