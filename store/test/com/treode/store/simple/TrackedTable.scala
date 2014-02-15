package com.treode.store.simple

import com.treode.async.{Callback, callback}

private class TrackedTable (table: TestTable, tracker: TrackingTable) extends TestTable {

  def get (key: Int, cb: Callback [Option [Int]]): Unit =
    table.get (key, cb)

  def iterator (cb: Callback [TestIterator]): Unit =
    table.iterator (cb)

  def put (key: Int, value: Int, cb: Callback [Unit]) {
    tracker.putting (key, value)
    table.put (key, value, callback (cb) { _ =>
      tracker.put (key, value)
    })
  }

  def delete (key: Int, cb: Callback [Unit]) {
    tracker.deleting (key)
    table.delete (key, callback (cb) { _ =>
      tracker.deleted (key)
    })
  }}
