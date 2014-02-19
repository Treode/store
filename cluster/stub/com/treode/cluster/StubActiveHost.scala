package com.treode.cluster

import scala.util.Random

import com.treode.async.Scheduler
import com.treode.pickle.Pickler

class StubActiveHost (val localId: HostId, network: StubNetwork) extends Cluster with StubHost {
  import network.random

  private val mailboxes: MailboxRegistry =
    new MailboxRegistry

  private val peers: PeerRegistry =
    new PeerRegistry (localId, new StubConnection (_, localId, network))

  def listen [M] (desc: MessageDescriptor [M]) (f: (M, Peer) => Any): Unit =
    mailboxes.listen (desc.pmsg, desc.id) (f)

  def open [M] (p: Pickler [M], s: Scheduler): EphemeralMailbox [M] =
    mailboxes.open (p, s)

  def peer (id: HostId): Peer =
    peers.get (id)

  def locate (id: Int): Acknowledgements =
    network.locate (id)

  def deliver [M] (p: Pickler [M], from: HostId, mbx: MailboxId, msg: M): Unit =
    mailboxes.deliver (p, peers.get (from), mbx, msg)
}
