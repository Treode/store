package com.treode.store.atomic

import java.nio.file.Paths
import scala.util.Random

import com.treode.async.{Callback, CallbackCaptor}
import com.treode.async.io.StubFile
import com.treode.cluster.{Cluster, HostId, StubActiveHost, StubNetwork}
import com.treode.store._
import com.treode.store.paxos.Paxos
import com.treode.disk.{Disks, DiskDriveConfig}
import com.treode.store.temp.TestableTempKit

private class StubAtomicHost (id: HostId, network: StubNetwork)
extends StubActiveHost (id, network) {
  import network.{random, scheduler}

  implicit val cluster: Cluster = this

  implicit val recovery = Disks.recover()
  implicit val storeConfig = StoreConfig (1<<16)
  val _paxos = CallbackCaptor [Paxos]
  Paxos.recover (_paxos)
  val file = new StubFile
  val config = DiskDriveConfig (10, 6, 1<<20)
  recovery.attach (Seq ((Paths.get ("a"), file, config)), Callback.ignore)
  scheduler.runTasks()
  while (!(_paxos.hasPassed || _paxos.hasFailed))
    Thread.sleep (1)

  implicit val paxos = _paxos.passed

  implicit val store = TestableTempKit (2)
  val atomic = new AtomicKit

  def writeDeputy (xid: TxId) = atomic.WriteDeputies.get (xid)

  def read (rt: TxClock, ops: Seq [ReadOp], cb: ReadCallback) =
    atomic.read (rt, ops, cb)

  def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback) =
    atomic.write (xid, ct, ops, cb)

  def expectCells (id: TableId) (cs: TimedCell*) = store.expectCells (id) (cs: _*)
}
