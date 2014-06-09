package com.treode.cluster

import java.lang.management.{ManagementFactory, OperatingSystemMXBean}
import java.net.SocketAddress
import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.Executors
import scala.util.Random

import com.treode.async.Scheduler
import com.treode.pickle.Pickler

trait Cluster {

  def localId: HostId

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

  private val osBean = ManagementFactory.getPlatformMXBean (classOf [OperatingSystemMXBean]);

  private def spreadLocalStats () (implicit scheduler: Scheduler, cluser: Cluster) {
    Peer.load.spread (osBean.getSystemLoadAverage)
    Peer.time.spread (System.currentTimeMillis)
    scheduler.delay (1000) (spreadLocalStats())
  }

  def live (
      cellId: CellId,
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

      val peers = PeerRegistry.live (cellId, localId, group, ports)

      val scuttlebutt = new Scuttlebutt (localId, peers)

      val listener = new Listener (cellId, localId, localAddr, group, peers)

      implicit val cluster  = new ClusterLive (localId, ports, peers, listener, scuttlebutt)

      Peer.load.listen { (load, peer) =>
        peer.load = load
      }

      Peer.time.listen { (time, peer) =>
        peer.time = time
      }

      scheduler.delay (1000) (spreadLocalStats())

      cluster

    } catch {

      case t: Throwable =>
        if (group != null)
          group.shutdownNow()
        throw t
    }}}
