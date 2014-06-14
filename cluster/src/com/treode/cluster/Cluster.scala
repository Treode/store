package com.treode.cluster

import java.lang.management.{ManagementFactory, OperatingSystemMXBean}
import java.net.SocketAddress
import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.Executors
import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.pickle.Pickler

trait Cluster {

  def cellId: CellId

  def localId: HostId

  def listen [M] (desc: MessageDescriptor [M]) (f: (M, Peer) => Any)

  def listen [M] (desc: RumorDescriptor [M]) (f: (M, Peer) => Any)

  def hail (remoteId: HostId, remoteAddr: SocketAddress)

  def peer (id: HostId): Peer

  def rpeer: Option [Peer]

  def open [M] (p: Pickler [M]) (f: (M, Peer) => Any): EphemeralPort [M]

  def spread [M] (desc: RumorDescriptor [M]) (msg: M)

  def startup()

  def shutdown(): Async [Unit]
}

object Cluster {

  private val osBean = ManagementFactory.getPlatformMXBean (classOf [OperatingSystemMXBean]);

  private def updateLocalStats () (implicit
      scheduler: Scheduler,
      cluser: Cluster,
      config: ClusterConfig
  ) {
    Peer.load.spread (osBean.getSystemLoadAverage)
    Peer.time.spread (System.currentTimeMillis)
    scheduler.delay (config.statsUpdatePeriod) (updateLocalStats())
  }

  def live (
      cellId: CellId,
      hostId: HostId,
      bindAddr: SocketAddress,
      shareAddr: SocketAddress
  ) (implicit
      random: Random,
      scheduler: Scheduler,
      config: ClusterConfig
  ): Cluster = {

    var group: AsynchronousChannelGroup = null

    try {

      val ports = new PortRegistry

      group = AsynchronousChannelGroup.withFixedThreadPool (1, Executors.defaultThreadFactory)

      val peers = PeerRegistry.live (cellId, hostId, group, ports)

      val scuttlebutt = new Scuttlebutt (hostId, peers)

      val listener = new Listener (cellId, hostId, bindAddr, group, peers)

      implicit val cluster  =
        new ClusterLive (cellId, hostId, group, ports, peers, listener, scuttlebutt)

      Peer.load.listen { (load, peer) =>
        peer.load = load
      }

      Peer.time.listen { (time, peer) =>
        peer.time = time
      }

      Peer.address.spread (shareAddr)
      updateLocalStats()

      cluster

    } catch {

      case t: Throwable =>
        if (group != null)
          group.shutdownNow()
        throw t
    }}}
