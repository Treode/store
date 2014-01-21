package com.treode.store.cluster.atomic

import java.nio.file.Paths
import scala.util.Random

import com.treode.async.Callback
import com.treode.async.io.StubFile
import com.treode.cluster.{BaseStubHost, HostId, StubCluster}
import com.treode.store._
import com.treode.store.cluster.paxos.PaxosKit
import com.treode.disk.{Disks, DiskDriveConfig}
import com.treode.store.local.temp.TestableTempKit

private class StubHost (id: HostId, cluster: StubCluster) extends BaseStubHost (id, cluster) {

  val disks = Disks (scheduler)
  val file = new StubFile (cluster.scheduler)
  val config = DiskDriveConfig (16, 8, 1L<<20)
  disks.attach (Seq ((Paths.get ("a"), file, config)), Callback.ignore)

  val store = TestableTempKit (2)

  val paxos = PaxosKit (this, disks)

  val atomic = new AtomicKit () (this, store, paxos)

  val mainDb = new TestableMainDb (atomic.WriteDeputies.mainDb, cluster.scheduler)

  def writeDeputy (xid: TxId) = atomic.WriteDeputies.get (xid)

  def read (rt: TxClock, ops: Seq [ReadOp], cb: ReadCallback) =
    atomic.read (rt, ops, cb)

  def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback) =
    atomic.write (xid, ct, ops, cb)

  def expectCells (id: TableId) (cs: TimedCell*) = store.expectCells (id) (cs: _*)
}
