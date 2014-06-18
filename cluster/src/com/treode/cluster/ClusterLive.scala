package com.treode.cluster

import java.net.SocketAddress
import java.nio.channels.AsynchronousChannelGroup

import com.treode.async.Async
import com.treode.pickle.Pickler

import Async.guard

private class ClusterLive (
    val cellId: CellId,
    val localId: HostId,
    group: AsynchronousChannelGroup,
    ports: PortRegistry,
    peers: PeerRegistry,
    listener: Listener,
    scuttlebutt: Scuttlebutt
) extends Cluster {

  def listen [M] (desc: MessageDescriptor [M]) (f: (M, Peer) => Any): Unit =
    ports.listen (desc.pmsg, desc.id) (f)

  def listen [M] (desc: RumorDescriptor [M]) (f: (M, Peer) => Any): Unit =
    scuttlebutt.listen (desc) (f)

  def hail (remoteId: HostId, remoteAddr: SocketAddress): Unit =
    peers.get (remoteId) .address = remoteAddr

  def startup(): Unit = {
    scuttlebutt.attach (this)
    listener.startup()
  }

  def shutdown(): Async [Unit] =
    guard {
      listener.shutdown()
      for {
        _ <- peers.shutdown()
      } yield {
        group.shutdownNow()
      }}

  def peer (id: HostId): Peer =
    peers.get (id)

  def rpeer: Option [Peer] =
    peers.rpeer

  def open [M] (p: Pickler [M]) (f: (M, Peer) => Any): EphemeralPort [M] =
    ports.open (p) (f)

  def spread [M] (desc: RumorDescriptor [M]) (msg: M): Unit =
    scuttlebutt.spread (desc) (msg)
}
