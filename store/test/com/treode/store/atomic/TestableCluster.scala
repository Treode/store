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
    randomHost.read (rt, ops)

  def write (ct: TxClock, ops: WriteOp*): Async [WriteResult] =
    randomHost.write (TxId (random.nextLong, 0), ct, ops)

  def expectCells (t: TableId) (expected: Cell*): Unit =
    hosts foreach (_.expectCells (t) (expected: _*))
}
