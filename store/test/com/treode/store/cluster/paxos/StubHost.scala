package com.treode.store.cluster.paxos

import com.treode.cluster.{BaseStubHost, HostId, StubCluster}

private class StubHost (id: HostId, cluster: StubCluster) extends BaseStubHost (id, cluster) {

  val paxos = new PaxosKit () (this)
}
