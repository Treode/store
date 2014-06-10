package com.treode.cluster

import java.net.{InetSocketAddress, SocketAddress}
import com.treode.pickle.{PickleContext, Pickler, Picklers, UnpickleContext}

private trait ClusterPicklers extends Picklers {

  lazy val socketAddress =
    wrap (tuple (option (string), uint))
    .build [SocketAddress] {
      case (Some (host), port) => new InetSocketAddress (host, port)
      case (None, port) => new InetSocketAddress (port)
    } .inspect {
      case addr: InetSocketAddress if addr.getAddress.isAnyLocalAddress =>
        (None, addr.getPort)
      case addr: InetSocketAddress =>
        (Some (addr.getHostString), addr.getPort)
      case addr =>
        throw new MatchError (addr)
    }

  def cellId = CellId.pickler
  def hostId = HostId.pickler
  def portId = PortId.pickler
  def rumorId = RumorId.pickler
}

private object ClusterPicklers extends ClusterPicklers
