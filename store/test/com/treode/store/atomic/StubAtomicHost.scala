package com.treode.store.atomic

import java.nio.file.Paths
import scala.util.Random

import com.treode.async.{Async, AsyncTestTools, Callback}
import com.treode.async.io.StubFile
import com.treode.cluster.{Cluster, HostId, StubActiveHost, StubNetwork}
import com.treode.store._
import com.treode.store.paxos.Paxos
import com.treode.disk.{Disks, DisksConfig, DiskGeometry}
import com.treode.store.temp.TestableTempKit

import AsyncTestTools._
import Callback.ignore

private class StubAtomicHost (id: HostId, network: StubNetwork)
extends StubActiveHost (id, network) {
  import network.{random, scheduler}

  implicit val cluster: Cluster = this

  implicit val disksConfig = DisksConfig (14, 1<<24, 1<<16, 10, 1)
  implicit val recovery = Disks.recover()
  implicit val storeConfig = StoreConfig (8, 1<<16)
  val _paxos = Paxos.recover() .capture()
  val file = new StubFile
  val geometry = DiskGeometry (10, 6, 1<<20)
  recovery.attach (Seq ((Paths.get ("a"), file, geometry))) .run (ignore)
  scheduler.runTasks()
  while (!(_paxos.hasPassed || _paxos.hasFailed))
    Thread.sleep (1)

  implicit val paxos = _paxos.passed

  implicit val store = new TestableTempKit
  val atomic = new AtomicKit

  def writeDeputy (xid: TxId) = atomic.WriteDeputies.get (xid)

  def read (rt: TxClock, ops: Seq [ReadOp]): Async [Seq [Value]] =
    atomic.read (rt, ops)

  def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp]): Async [WriteResult] =
    atomic.write (xid, ct, ops)

  def expectCells (id: TableId) (cs: TimedCell*) = store.expectCells (id) (cs: _*)
}
