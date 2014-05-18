package com.treode.store.tier

import com.treode.async.{Async, AsyncIterator}
import com.treode.async.implicits._

private class TrackedTable (table: TestTable, tracker: TableTracker) {

  def get (key: Int): Async [Option [Int]] =
    table.get (key)

  def iterator: AsyncIterator [TestCell] =
    table.iterator

  def put (key: Int, value: Int): Async [Unit] = {
    tracker.putting (key, value)
    table.put (key, value) .map (_ => tracker.put (key, value))
  }

  def putAll (kvs: (Int, Int)*): Async [Unit] =
    for ((key, value) <- kvs.latch.unit)
      put (key, value)

  def delete (key: Int): Async [Unit] = {
    tracker.deleting (key)
    table.delete (key) .map (_ => tracker.deleted (key))
  }}
