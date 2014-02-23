package com.treode.store.atomic

import java.nio.file.Paths
import scala.util.Random

import com.treode.async.{Async, AsyncTestTools, Callback, CallbackCaptor}
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
  implicit val storeConfig = StoreConfig (8, 1<<16)

  implicit val recovery = Disks.recover()
  implicit val store = new TestableTempKit
  val _paxos = Paxos.recover()
  val _atomic = AtomicKit.recover()

  val file = new StubFile
  val geometry = DiskGeometry (10, 6, 1<<20)
  val files = Seq ((Paths.get ("a"), file, geometry))

  val _launch =
    for {
      launch <- recovery.attach (files)
      paxos <- _paxos.launch (launch)
      atomic <- _atomic.launch (launch, paxos)
    } yield {
      launch.launch()
      (launch.disks, paxos, atomic)
    }

  val captor = _launch.capture()
  scheduler.runTasks()
  while (!captor.wasInvoked)
    Thread.sleep (10)
  implicit val (disks, paxos, atomic) = captor.passed

  def writeDeputy (xid: TxId) = atomic.WriteDeputies.get (xid)

  def read (rt: TxClock, ops: Seq [ReadOp]): Async [Seq [Value]] =
    atomic.read (rt, ops)

  def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp]): Async [WriteResult] =
    atomic.write (xid, ct, ops)

  def expectCells (id: TableId) (cs: TimedCell*) = store.expectCells (id) (cs: _*)
}
