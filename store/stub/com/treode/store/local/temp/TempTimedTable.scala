package com.treode.store.local.temp

import java.util.concurrent.ConcurrentSkipListSet

import com.treode.async.Callback
import com.treode.store.{Bytes, TimedCell, TxClock}
import com.treode.store.local.{TimedReader, TimedTable, TimedWriter}

private class TempTimedTable extends TimedTable {

  // Visible for testing.
  protected val memtable = new ConcurrentSkipListSet [TimedCell] (TimedCell)

  def get (key: Bytes, time: TxClock, cb: Callback [TimedCell]): Unit =
    Callback.guard (cb) {
      val c = memtable.ceiling (TimedCell (key, time, None))
      if (c == null)
        cb (TimedCell (key, TxClock.zero, None))
      else
        cb (c)
    }

  def put (key: Bytes, time: TxClock, value: Option [Bytes], cb: Callback [Unit]): Unit =
    Callback.guard (cb) {
      memtable.add (TimedCell (key, time, value))
      cb()
    }

  def close() = ()
}
