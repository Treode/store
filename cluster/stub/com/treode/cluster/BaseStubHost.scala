package com.treode.cluster

import scala.util.Random

import com.treode.async.{Scheduler, StubScheduler}
import com.treode.buffer.PagedBuffer
import com.treode.cluster.events.StubEvents
import com.treode.cluster.messenger.{MailboxRegistry, PeerRegistry}
import com.treode.pickle.{Pickler, pickle}

class BaseStubHost (val localId: HostId, cluster: StubCluster)
extends Host with StubHost {

  val random: Random = cluster.random

  val scheduler: Scheduler = cluster.scheduler

  val mailboxes: MailboxRegistry = new MailboxRegistry () (StubEvents)

  val peers: PeerRegistry =
    new PeerRegistry (localId, new StubConnection (_, localId, cluster)) (random)

  def locate (id: Int): Acknowledgements =
    cluster.locate (id)

  def deliver [M] (p: Pickler [M], from: HostId, mbx: MailboxId, msg: M): Unit =
    mailboxes.deliver (p, peers.get (from), mbx, msg)
}
