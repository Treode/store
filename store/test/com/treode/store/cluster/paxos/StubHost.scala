package com.treode.store.cluster.paxos

import java.nio.file.Paths
import com.treode.async.Callback
import com.treode.async.io.StubFile
import com.treode.cluster.{BaseStubHost, HostId, StubCluster}
import com.treode.cluster.events.StubEvents
import com.treode.store.disk2.{Disks, DiskDriveConfig}

private class StubHost (id: HostId, cluster: StubCluster) extends BaseStubHost (id, cluster) {

  val disks = Disks (scheduler, StubEvents)
  val file = new StubFile (cluster.scheduler)
  val config = DiskDriveConfig (16, 8, 1L<<20)
  disks.attach (Seq ((Paths.get ("a"), file, config)), Callback.ignore)

  val paxos = new PaxosKit () (this, disks)
}
