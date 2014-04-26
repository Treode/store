package com.treode.store.atomic

import com.treode.async.Async
import com.treode.cluster.StubNetwork
import com.treode.store._

private class TestableCluster (hosts: Seq [StubAtomicHost], network: StubNetwork)
extends TestableStore {
  import network.random

  private def randomHost: StubAtomicHost =
    hosts (random.nextInt (hosts.size))

  def read (rt: TxClock, ops: ReadOp*): Async [Seq [Value]] =
    randomHost.read (rt, ops:_*)

  def write (ct: TxClock, ops: WriteOp*): Async [TxClock] =
    randomHost.write (TxId (random.nextLong, 0), ct, ops:_*)

  def expectCells (t: TableId) (expected: Cell*): Unit =
    hosts foreach (_.expectCells (t) (expected: _*))
}
