package com.treode.store.local.temp

import java.util.concurrent.ConcurrentSkipListSet

import com.treode.concurrent.Callback
import com.treode.store.{Bytes, TxClock}
import com.treode.store.local.{TimedCell, TimedReader, TimedTable, TimedWriter}

private class TempTimedTable extends TimedTable {

  // Visible for testing.
  protected val memtable = new ConcurrentSkipListSet [TimedCell] (TimedCell)

  def read (key: Bytes, n: Int, reader: TimedReader) {
    val c = memtable.ceiling (TimedCell (key, reader.rt, None))
    if (c == null)
      reader.got (n, TimedCell (key, TxClock.zero, None))
    else
      reader.got (n, c)
  }

  private def commit (wt: TxClock, key: Bytes, value: Option [Bytes]) {
    memtable.add (TimedCell (key, wt, value))
  }

  def create (key: Bytes, value: Bytes, n: Int, writer: TimedWriter) {
    val c = memtable.ceiling (TimedCell (key, TxClock.max, None))
    if (c == null || c.value.isEmpty)
      writer.prepare (commit (_, key, Some (value)))
    else
      writer.conflict (n)
  }

  def hold (key: Bytes, writer: TimedWriter) {
    val c = memtable.ceiling (TimedCell (key, TxClock.max, None))
    if (c == null || c.time <= writer.ct)
      writer.prepare()
    else
      writer.advance (c.time)
  }

  def update (key: Bytes, value: Bytes, writer: TimedWriter) {
    val c = memtable.ceiling (TimedCell (key, TxClock.max, None))
    if (c != null && writer.ct < c.time)
      writer.advance (c.time)
    else if (c != null && Some (value) == c.value)
      writer.prepare()
    else
      writer.prepare (commit (_, key, Some (value)))
  }

  def delete (key: Bytes, writer: TimedWriter) {
    val c = memtable.ceiling (TimedCell (key, TxClock.max, None))
    if (c != null && writer.ct < c.time)
      writer.advance (c.time)
    else if (c == null || c.value.isEmpty)
      writer.prepare()
    else
      writer.prepare (commit (_, key, None))
  }

  def close() = ()
}
