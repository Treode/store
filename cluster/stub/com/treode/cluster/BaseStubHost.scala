package com.treode.cluster

import scala.util.Random

import com.treode.async.{Scheduler, StubScheduler}
import com.treode.buffer.PagedBuffer
import com.treode.cluster.events.StubEvents
import com.treode.cluster.messenger.{MailboxRegistry, PeerRegistry}
import com.treode.pickle.{Pickler, pickle}

class BaseStubHost (val localId: HostId, cluster: StubCluster)
extends Host with StubHost {

  private val mailboxes: MailboxRegistry =
    new MailboxRegistry

  private val peers: PeerRegistry =
    new PeerRegistry (localId, new StubConnection (_, localId, cluster)) (cluster.random)

  def register [M] (desc: MessageDescriptor [M]) (f: (M, Peer) => Any): Unit =
    mailboxes.register (desc.pmsg, desc.id) (f)

  def open [M] (p: Pickler [M], s: Scheduler): EphemeralMailbox [M] =
    mailboxes.open (p, s)

  def peer (id: HostId): Peer =
    peers.get (id)

  def locate (id: Int): Acknowledgements =
    cluster.locate (id)

  def deliver [M] (p: Pickler [M], from: HostId, mbx: MailboxId, msg: M): Unit =
    mailboxes.deliver (p, peers.get (from), mbx, msg)
}
