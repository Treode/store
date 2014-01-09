package com.treode.store.cluster.atomic

import java.nio.file.Paths
import scala.util.Random

import com.treode.cluster.{BaseStubCluster, HostId}
import com.treode.store._
import com.treode.store.cluster.paxos.PaxosKit
import com.treode.store.local.temp.TestableTempKit

private class StubCluster (seed: Long, nhosts: Int, multithreaded: Boolean = false)
extends BaseStubCluster (seed, nhosts, multithreaded) with TestableStore {

  class StubHost (id: HostId) extends BaseStubHost (id) {

    val store = TestableTempKit (2)

    val paxos = PaxosKit (StubHost.this)

    val atomic = new AtomicKit () (StubHost.this, store, paxos)

    val mainDb = new TestableMainDb (atomic.WriteDeputies.mainDb, StubCluster.this.scheduler)

    def writeDeputy (xid: TxId) = atomic.WriteDeputies.get (xid)

    def read (rt: TxClock, ops: Seq [ReadOp], cb: ReadCallback) =
      atomic.read (rt, ops, cb)

    def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback) =
      atomic.write (xid, ct, ops, cb)

    def expectCells (id: TableId) (cs: TimedCell*) = store.expectCells (id) (cs: _*)

    override def cleanup() {
      atomic.close()
      paxos.close()
      store.close()
    }}

  def newHost (id: HostId) = new StubHost (id)

  private def randomHost: StubHost =
    hosts (random.nextInt (hosts.size))

  def read (rt: TxClock, ops: Seq [ReadOp], cb: ReadCallback): Unit =
    randomHost.read (rt, ops, cb)

  def write (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback): Unit =
    randomHost.write (TxId (random.nextLong), ct, ops, cb)

  def expectCells (t: TableId) (expected: TimedCell*): Unit =
    hosts foreach (_.expectCells (t) (expected: _*))
}
