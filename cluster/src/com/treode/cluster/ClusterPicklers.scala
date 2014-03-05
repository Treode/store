package com.treode.cluster

import com.treode.pickle.Picklers

private trait ClusterPicklers extends Picklers {

  def hostId = HostId.pickler
  def portId = PortId.pickler
}

private object ClusterPicklers extends ClusterPicklers
