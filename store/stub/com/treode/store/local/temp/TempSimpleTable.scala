package com.treode.store.local.temp

import java.util.concurrent.ConcurrentSkipListMap

import com.treode.async.{Callback, guard}
import com.treode.store.{Bytes, SimpleTable}

private class TempSimpleTable extends SimpleTable {

  protected val memtable = new ConcurrentSkipListMap [Bytes, Bytes]

  def get (key: Bytes, cb: Callback [Option [Bytes]]): Unit =
    guard (cb) {
      val value = memtable.get (key)
      if (value == null)
        cb (None)
      else
        cb (Some (value))
    }

  def put (key: Bytes, value: Bytes, cb: Callback [Unit]): Unit =
    guard (cb) {
      memtable.put (key, value)
      cb()
    }

  def del (key: Bytes, cb: Callback [Unit]): Unit =
    guard (cb) {
      memtable.remove (key)
      cb()
    }

  def close() = ()
}
