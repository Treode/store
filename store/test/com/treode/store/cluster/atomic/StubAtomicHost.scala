package com.treode.store.cluster.atomic

import java.nio.file.Paths
import scala.util.Random

import com.treode.async.Callback
import com.treode.async.io.StubFile
import com.treode.cluster.{Cluster, HostId, StubActiveHost, StubNetwork}
import com.treode.store._
import com.treode.store.cluster.paxos.PaxosKit
import com.treode.disk.{Disks, DiskDriveConfig}
import com.treode.store.local.temp.TestableTempKit

private class StubAtomicHost (id: HostId, network: StubNetwork)
extends StubActiveHost (id, network) {
  import network.{random, scheduler}

  implicit val disks = Disks()
  val file = new StubFile
  val config = DiskDriveConfig (16, 8, 1L<<20)
  disks.attach (Seq ((Paths.get ("a"), file, config)), Callback.ignore)

  implicit val cluster: Cluster = this

  implicit val store = TestableTempKit (2)

  implicit val paxos = PaxosKit()

  val atomic = new AtomicKit

  val mainDb = new TestableMainDb (atomic.WriteDeputies.mainDb, scheduler)

  def writeDeputy (xid: TxId) = atomic.WriteDeputies.get (xid)

  def read (rt: TxClock, ops: Seq [ReadOp], cb: ReadCallback) =
    atomic.read (rt, ops, cb)

  def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback) =
    atomic.write (xid, ct, ops, cb)

  def expectCells (id: TableId) (cs: TimedCell*) = store.expectCells (id) (cs: _*)
}
