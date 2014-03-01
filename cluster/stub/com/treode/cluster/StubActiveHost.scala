package com.treode.cluster

import java.net.SocketAddress
import scala.util.Random

import com.treode.async.Scheduler
import com.treode.pickle.Pickler

class StubActiveHost (val localId: HostId, network: StubNetwork) extends Cluster with StubHost {
  import network.{random, scheduler}

  private val mailboxes: MailboxRegistry =
    new MailboxRegistry

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
    mailboxes.listen (desc.pmsg, desc.id) (f)

  def listen [M] (desc: RumorDescriptor [M]) (f: (M, Peer) => Any): Unit =
    scuttlebutt.listen (desc) (f)

  def open [M] (p: Pickler [M], s: Scheduler): EphemeralMailbox [M] =
    mailboxes.open (p, s)

  def spread [M] (desc: RumorDescriptor [M]) (msg: M): Unit =
    scuttlebutt.spread (desc) (msg)

  def locate (id: Int): Acknowledgements =
    network.locate (id)

  def deliver [M] (p: Pickler [M], from: HostId, mbx: MailboxId, msg: M): Unit =
    mailboxes.deliver (p, peers.get (from), mbx, msg)
}
