package com.treode.store.paxos

import java.nio.file.Paths
import com.treode.async.{AsyncTestTools, CallbackCaptor}
import com.treode.async.io.StubFile
import com.treode.cluster.{Cluster, HostId, StubActiveHost, StubNetwork}
import com.treode.disk.{Disks, DisksConfig, DiskGeometry}
import com.treode.store.StoreConfig

import AsyncTestTools._

private class StubPaxosHost (id: HostId, network: StubNetwork)
extends StubActiveHost (id, network) {
  import network.{random, scheduler}

  implicit val cluster: Cluster = this

  implicit val disksConfig = DisksConfig (14, 1<<24, 1<<16, 10, 1)
  implicit val recovery = Disks.recover()
  implicit val storeConfig = StoreConfig (1<<16)
  val _paxos = CallbackCaptor [Paxos]
  Paxos.recover (_paxos)
  val file = new StubFile
  val geometry = DiskGeometry (10, 6, 1<<20)
  recovery.attach (Seq ((Paths.get ("a"), file, geometry))) .pass

  val paxos = _paxos.passed
  val acceptors = paxos.asInstanceOf [PaxosKit] .acceptors
}
