package com.treode.store.tier

import com.treode.async.{Async, AsyncIterator, Callback, callback}

import Async.async

private class TrackedTable (table: TestTable, tracker: TrackingTable) extends TestTable {

  def get (key: Int): Async [Option [Int]] =
    table.get (key)

  def iterator: AsyncIterator [TestCell] =
    table.iterator

  def put (key: Int, value: Int): Async [Unit] = {
    tracker.putting (key, value)
    table.put (key, value) .map (_ => tracker.put (key, value))
  }

  def delete (key: Int): Async [Unit] = {
    tracker.deleting (key)
    table.delete (key) .map (_ => tracker.deleted (key))
  }}
