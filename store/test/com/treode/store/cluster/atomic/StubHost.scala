package com.treode.store.cluster.atomic

import java.nio.file.Paths
import scala.util.Random

import com.treode.cluster.{BaseStubHost, HostId, StubCluster}
import com.treode.store._
import com.treode.store.cluster.paxos.PaxosKit
import com.treode.store.local.temp.TestableTempKit

private class StubHost (id: HostId, cluster: StubCluster) extends BaseStubHost (id, cluster) {

  val store = TestableTempKit (2)

  val paxos = PaxosKit (this)

  val atomic = new AtomicKit () (this, store, paxos)

  val mainDb = new TestableMainDb (atomic.WriteDeputies.mainDb, cluster.scheduler)

  def writeDeputy (xid: TxId) = atomic.WriteDeputies.get (xid)

  def read (rt: TxClock, ops: Seq [ReadOp], cb: ReadCallback) =
    atomic.read (rt, ops, cb)

  def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback) =
    atomic.write (xid, ct, ops, cb)

  def expectCells (id: TableId) (cs: TimedCell*) = store.expectCells (id) (cs: _*)
}
