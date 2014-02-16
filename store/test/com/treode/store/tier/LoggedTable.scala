package com.treode.store.tier

import com.treode.async.{Async, AsyncIterator, Callback, callback}
import com.treode.disk.Disks
import com.treode.store.Bytes

import Async.async

private class LoggedTable (table: TierTable) (implicit disks: Disks) extends TestTable {

  def get (key: Int): Async [Option [Int]] =
    table.get (Bytes (key)) .map (_.map (_.int))

  def iterator: AsyncIterator [TestCell] =
    table.iterator.map (new TestCell (_))

  def put (key: Int, value: Int): Async [Unit] = {
    val gen = table.put (Bytes (key), Bytes (value))
    TestTable.put.record (gen, key, value)
  }

  def delete (key: Int): Async [Unit] = {
    val gen = table.delete (Bytes (key))
    TestTable.delete.record (gen, key)
  }}
