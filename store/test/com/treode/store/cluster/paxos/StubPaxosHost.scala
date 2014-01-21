package com.treode.store.cluster.paxos

import java.nio.file.Paths
import com.treode.async.Callback
import com.treode.async.io.StubFile
import com.treode.cluster.{Cluster, HostId, StubActiveHost, StubNetwork}
import com.treode.disk.{Disks, DiskDriveConfig}

private class StubPaxosHost (id: HostId, network: StubNetwork)
extends StubActiveHost (id, network) {
  import network.{random, scheduler}

  implicit val disks = Disks()
  val file = new StubFile
  val config = DiskDriveConfig (16, 8, 1L<<20)
  disks.attach (Seq ((Paths.get ("a"), file, config)), Callback.ignore)

  implicit val cluster: Cluster = this

  val paxos = new PaxosKit
}
