package com.treode.store

import java.net.SocketAddress

import com.treode.cluster.HostId
import com.treode.cluster.stubs.StubCluster

abstract class StubStoreHost (val localId: HostId) (implicit kit: StoreTestKit) {
  import kit._

  implicit val cluster = new StubCluster (localId)

  def hail (remoteId: HostId, remoteAddr: SocketAddress): Unit =
    cluster.hail (remoteId, remoteAddr)
}
