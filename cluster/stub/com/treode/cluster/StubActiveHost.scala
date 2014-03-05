package com.treode.cluster

import java.net.SocketAddress
import scala.util.Random

import com.treode.async.Scheduler
import com.treode.pickle.Pickler

class StubActiveHost (val localId: HostId, network: StubNetwork) extends Cluster with StubHost {
  import network.{random, scheduler}

  private val ports: PortRegistry =
    new PortRegistry

  private val peers: PeerRegistry =
    new PeerRegistry (localId, new StubConnection (_, localId, network))

  val scuttlebutt: Scuttlebutt =
    new Scuttlebutt (localId, peers)

  def hail (remoteId: HostId, remoteAddr: SocketAddress): Unit =
    peers.get (remoteId) .address = remoteAddr

  def peer (id: HostId): Peer =
    peers.get (id)

  def rpeer: Option [Peer] =
    peers.rpeer

  def listen [M] (desc: MessageDescriptor [M]) (f: (M, Peer) => Any): Unit =
    ports.listen (desc.pmsg, desc.id) (f)

  def listen [M] (desc: RumorDescriptor [M]) (f: (M, Peer) => Any): Unit =
    scuttlebutt.listen (desc) (f)

  def open [M] (p: Pickler [M]) (f: (M, Peer) => Any): EphemeralPort [M] =
    ports.open (p) (f)

  def spread [M] (desc: RumorDescriptor [M]) (msg: M): Unit =
    scuttlebutt.spread (desc) (msg)

  def deliver [M] (p: Pickler [M], from: HostId, port: PortId, msg: M): Unit =
    ports.deliver (p, peers.get (from), port, msg)
}
