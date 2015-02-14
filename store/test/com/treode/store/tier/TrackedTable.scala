/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.store.tier

import com.treode.async.{Async, BatchIterator}
import com.treode.async.implicits._

private class TrackedTable (table: TestTable, tracker: TableTracker) {

  def get (key: Int): Async [Option [Int]] =
    table.get (key)

  def iterator: BatchIterator [TestCell] =
    table.iterator

  def put (key: Int, value: Int): Async [Unit] = {
    tracker.putting (key, value)
    table.put (key, value) .map (_ => tracker.put (key, value))
  }

  def putAll (kvs: (Int, Int)*): Async [Unit] =
    for ((key, value) <- kvs.latch)
      put (key, value)

  def delete (key: Int): Async [Unit] = {
    tracker.deleting (key)
    table.delete (key) .map (_ => tracker.deleted (key))
  }}
