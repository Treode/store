package com.treode.store.atomic

import scala.util.Random

import com.treode.async.Async
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

  def scan (table: TableId, start: Bound [Key], window: TimeBounds, slice: Slice): CellIterator =
    randomHost.scan (table, start, window, slice)
}
