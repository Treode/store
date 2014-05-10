package com.treode.store

import com.treode.cluster.{Cluster, HostId}

trait StubStoreHost {

  def localId: HostId

  def cluster: Cluster

  def hail (remoteId: HostId): Unit =
    cluster.hail (remoteId, null)
}
