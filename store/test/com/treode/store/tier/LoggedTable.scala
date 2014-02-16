package com.treode.store.tier

import com.treode.async.{AsyncIterator, Callback, callback}
import com.treode.disk.Disks
import com.treode.store.Bytes

private class LoggedTable (table: TierTable) (implicit disks: Disks) extends TestTable {

  def get (key: Int, cb: Callback [Option [Int]]): Unit =
    table.get (Bytes (key), callback (cb) { bytes =>
      bytes.map (_.int)
    })

  def iterator: AsyncIterator [TestCell] =
    table.iterator.map (new TestCell (_))

  def put (key: Int, value: Int, cb: Callback [Unit]) {
    val gen = table.put (Bytes (key), Bytes (value))
    TestTable.put.record (gen, key, value) (cb)
  }

  def delete (key: Int, cb: Callback [Unit]) {
    val gen = table.delete (Bytes (key))
    TestTable.delete.record (gen, key) (cb)
  }}
