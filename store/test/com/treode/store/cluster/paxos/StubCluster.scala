package com.treode.store.cluster.paxos

import java.nio.file.Paths
import scala.util.Random

import com.treode.cluster.{BaseStubCluster, HostId}
import com.treode.store.local.temp.TestableTempKit

private class StubCluster (seed: Long, nhosts: Int) extends BaseStubCluster (seed, nhosts) {

  class StubHost (id: HostId) extends BaseStubHost (id) {

    val paxos = new PaxosKit () (StubHost.this)

    override def cleanup() {
      paxos.close()
    }}

  def newHost (id: HostId) = new StubHost (id)
}
