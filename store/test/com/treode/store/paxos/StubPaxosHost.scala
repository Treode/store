package com.treode.store.paxos

import java.nio.file.Paths
import com.treode.async.{Callback, CallbackCaptor}
import com.treode.async.io.StubFile
import com.treode.cluster.{Cluster, HostId, StubActiveHost, StubNetwork}
import com.treode.disk.{Disks, DiskDriveConfig}
import com.treode.store.StoreConfig

private class StubPaxosHost (id: HostId, network: StubNetwork)
extends StubActiveHost (id, network) {
  import network.{random, scheduler}

  implicit val disks = Disks()

  implicit val cluster: Cluster = this

  implicit val storeConfig = StoreConfig (1<<16)

  private val _acceptors = new CallbackCaptor [Acceptors]
  def acceptors = _acceptors.passed
  val paxos = new PaxosKit
  Acceptors.attach (paxos, _acceptors)

  val file = new StubFile
  val config = DiskDriveConfig (16, 8, 1L<<20)
  disks.attach (Seq ((Paths.get ("a"), file, config)), Callback.ignore)
}
