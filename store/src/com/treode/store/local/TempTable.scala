package com.treode.store.local

import java.util.concurrent.ConcurrentSkipListSet

import com.treode.cluster.concurrent.Callback
import com.treode.store.{Bytes, Cell, TxClock}
import com.treode.store.tier.{BlockCache, Tier}

private class TempTable extends Table {

  // Visible for testing.
  protected val memtable = new ConcurrentSkipListSet [Cell] (Cell)

  def read (key: Bytes, n: Int, reader: Reader) {
    val c = memtable.ceiling (Cell (key, reader.rt, None))
    if (c == null)
      reader.got (n, Cell (key, TxClock.Zero, None))
    else
      reader.got (n, c)
  }

  private def commit (wt: TxClock, key: Bytes, value: Option [Bytes]) {
    memtable.add (Cell (key, wt, value))
  }

  def create (key: Bytes, value: Bytes, n: Int, writer: Writer) {
    val c = memtable.ceiling (Cell (key, TxClock.MaxValue, None))
    if (c == null || c.value.isEmpty)
      writer.prepare (commit (_, key, Some (value)))
    else
      writer.conflict (n)
  }

  def hold (key: Bytes, writer: Writer) {
    val c = memtable.ceiling (Cell (key, TxClock.MaxValue, None))
    if (c == null || c.time <= writer.ct)
      writer.prepare()
    else
      writer.advance (c.time)
  }

  def update (key: Bytes, value: Bytes, writer: Writer) {
    val c = memtable.ceiling (Cell (key, TxClock.MaxValue, None))
    if (c != null && writer.ct < c.time)
      writer.advance (c.time)
    else if (c != null && Some (value) == c.value)
      writer.prepare()
    else
      writer.prepare (commit (_, key, Some (value)))
  }

  def delete (key: Bytes, writer: Writer) {
    val c = memtable.ceiling (Cell (key, TxClock.MaxValue, None))
    if (c != null && writer.ct < c.time)
      writer.advance (c.time)
    else if (c == null || c.value.isEmpty)
      writer.prepare()
    else
      writer.prepare (commit (_, key, None))
  }}
