package com.treode.store.tier

import com.treode.async.{Async, AsyncIterator}
import com.treode.disk.Disks
import com.treode.store.{Bytes, TxClock}

import Async.guard

private class LoggedTable (table: TierTable) (implicit disks: Disks) extends TestTable {

  def get (key: Int): Async [Option [Int]] = guard {
    table.get (Bytes (key)) .map (_.map (_.int))
  }

  def iterator: AsyncIterator [TestCell] =
    table.iterator.map (new TestCell (_))

  def put (key: Int, value: Int): Async [Unit] = guard {
    val gen = table.put (Bytes (key), TxClock.zero, Bytes (value))
    TestTable.put.record (gen, key, value)
  }

  def delete (key: Int): Async [Unit] = guard {
    val gen = table.delete (Bytes (key), TxClock.zero)
    TestTable.delete.record (gen, key)
  }

  def checkpoint(): Async [Unit] = guard {
    for {
      meta <- table.checkpoint()
      _ <- TestTable.checkpoint.record (meta)
    } yield ()
  }}
