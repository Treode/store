package com.treode.store.cluster.atomic

import java.nio.file.Paths
import scala.util.Random

import com.treode.cluster.{BaseStubCluster, HostId}
import com.treode.store.{TableId, TimedCell, TxClock, TxId, WriteCallback, WriteOp}
import com.treode.store.cluster.paxos.PaxosKit
import com.treode.store.local.temp.TestableTempKit

private class StubCluster (seed: Long, nhosts: Int) extends BaseStubCluster (seed, nhosts) {

  class StubHost (id: HostId) extends BaseStubHost (id) {

    val store = TestableTempKit (2)

    val paxos = PaxosKit (StubHost.this, store)

    val atomic = new AtomicKit () (StubHost.this, store, paxos)

    val mainDb = new TestableMainDb (atomic.Deputies.mainDb, StubCluster.this.scheduler)

    def deputy (xid: TxId) =
      atomic.Deputies.get (xid)

    def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback) =
      atomic.write (xid, ct, ops, cb)

    def expectCells (id: TableId) (cs: TimedCell*) = store.expectCells (id) (cs: _*)

    override def cleanup() {
      atomic.close()
      paxos.close()
      store.close()
    }}

  def newHost (id: HostId) = new StubHost (id)
}
