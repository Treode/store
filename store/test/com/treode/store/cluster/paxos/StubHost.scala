package com.treode.store.cluster.paxos

import java.nio.file.Paths
import com.treode.async.Callback
import com.treode.async.io.StubFile
import com.treode.cluster.{BaseStubHost, Host, HostId, StubNetwork}
import com.treode.disk.{Disks, DiskDriveConfig}

private class StubHost (id: HostId, network: StubNetwork) extends BaseStubHost (id, network) {
  import network.{random, scheduler}

  implicit val disks = Disks()
  val file = new StubFile
  val config = DiskDriveConfig (16, 8, 1L<<20)
  disks.attach (Seq ((Paths.get ("a"), file, config)), Callback.ignore)

  implicit val host: Host = this

  val paxos = new PaxosKit
}
