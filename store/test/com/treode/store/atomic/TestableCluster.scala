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

package com.treode.store.atomic

import scala.util.Random

import com.treode.async.Async
import com.treode.cluster.HostId
import com.treode.cluster.stubs.StubNetwork
import com.treode.store._

import StoreTestTools._

private class TestableCluster (hosts: Seq [StubAtomicHost]) (implicit kit: StoreTestKit)
extends Store {
  import kit.{random, scheduler}

  private def randomHost: StubAtomicHost =
    hosts (random.nextInt (hosts.size))

  def read (rt: TxClock, ops: ReadOp*): Async [Seq [Value]] =
    randomHost.read (rt, ops:_*)

  def write (xid: TxId, ct: TxClock, ops: WriteOp*): Async [TxClock] =
    randomHost.write (xid, ct, ops:_*)

  def status (xid: TxId): Async [TxStatus] =
    randomHost.status (xid)

  def scan (table: TableId, start: Bound [Key], window: Window, slice: Slice): CellIterator =
    randomHost.scan (table, start, window, slice)

  def hosts (slice: Slice): Seq [(HostId, Int)] =
    randomHost.hosts (slice)
}
