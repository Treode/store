package com.treode.store.tier

import com.treode.async.{Async, AsyncIterator, Scheduler}
import com.treode.disk.{Disk, ObjectId, PageHandler}
import com.treode.store.{Bytes, Residents, TxClock}

import Async.{guard, supply}
import TierTestTools._

private class LoggedTable (table: TierTable) (implicit disks: Disk)
extends TestTable with PageHandler [Long] {

  def get (key: Int): Async [Option [Int]] = guard {
    for (cell <- table.get (Bytes (key), TxClock.max))
      yield cell.value.map (_.int)
  }

  def iterator: AsyncIterator [TestCell] =
    table.iterator (Residents.all) .map (new TestCell (_))

  def put (key: Int, value: Int): Async [Unit] = guard {
    val gen = table.put (Bytes (key), TxClock.zero, Bytes (value))
    TestTable.put.record (gen, key, value)
  }

  def delete (key: Int): Async [Unit] = guard {
    val gen = table.delete (Bytes (key), TxClock.zero)
    TestTable.delete.record (gen, key)
  }

  def probe (obj: ObjectId, groups: Set [Long]): Async [Set [Long]] = guard {
    table.probe (groups)
  }

  def compact (obj: ObjectId, groups: Set [Long]): Async [Unit] = guard {
    for {
      meta <- table.compact (groups, Residents.all)
      _ <- TestTable.checkpoint.record (meta)
    } yield ()
  }

  def checkpoint(): Async [Unit] = guard {
    for {
      meta <- table.checkpoint (Residents.all)
      _ <- TestTable.checkpoint.record (meta)
    } yield ()
  }}
