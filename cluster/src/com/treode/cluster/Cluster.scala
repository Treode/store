package com.treode.cluster

import java.net.SocketAddress
import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.Executors
import scala.util.Random

import com.treode.async.Scheduler
import com.treode.pickle.Pickler

trait Cluster {

  def listen [M] (desc: MessageDescriptor [M]) (f: (M, Peer) => Any)

  def listen [M] (desc: RumorDescriptor [M]) (f: (M, Peer) => Any)

  def hail (remoteId: HostId, remoteAddr: SocketAddress)

  def startup()

  def peer (id: HostId): Peer

  def rpeer: Option [Peer]

  def open [M] (p: Pickler [M]) (f: (M, Peer) => Any): EphemeralPort [M]

  def spread [M] (desc: RumorDescriptor [M]) (msg: M)
}

object Cluster {

  def live (
      localId: HostId,
      localAddr: SocketAddress
  ) (implicit
      random: Random,
      scheduler: Scheduler
  ): Cluster = {

    var group: AsynchronousChannelGroup = null

    try {

      val ports = new PortRegistry

      group = AsynchronousChannelGroup.withFixedThreadPool (1, Executors.defaultThreadFactory)

      val peers = PeerRegistry.live (localId, group, ports)

      val scuttlebutt = new Scuttlebutt (localId, peers)

      val listener = new Listener (localId, localAddr, group, peers)

      new ClusterLive (ports, peers, listener, scuttlebutt)

    } catch {

      case t: Throwable =>
        if (group != null)
          group.shutdownNow()
        throw t
    }}}
