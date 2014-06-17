package com.treode.cluster

import java.net.{InetSocketAddress, SocketAddress}
import com.treode.pickle.{PickleContext, Pickler, Picklers, UnpickleContext}

private trait ClusterPicklers extends Picklers {

  def cellId = CellId.pickler
  def hostId = HostId.pickler
  def portId = PortId.pickler
  def rumorId = RumorId.pickler
}

private object ClusterPicklers extends ClusterPicklers
